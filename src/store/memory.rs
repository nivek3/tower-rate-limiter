//! Memory fixed-window rate-limit store.

use moka::{ops::compute::Op, policy::Expiry, sync::Cache};
use std::{
    future::{Ready, ready},
    time::{Duration, Instant},
};

use crate::{FixedWindowStore, FixedWindowUsage, RateLimitError};

/// Errors returned by the in-memory rate limit store.
///
/// # Examples
///
/// ```rust
/// use tower_rate_limiter::MemoryStoreError;
///
/// let err = MemoryStoreError::InstantOutOfRange;
/// // does something
/// assert_eq!(err, MemoryStoreError::InstantOutOfRange);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum MemoryStoreError {
    #[error("instant is out of range")]
    InstantOutOfRange,

    #[error("failed to compute entry")]
    FailedToCompute,
}

/// Convert the MemoryStoreError to a RateLimitError.
impl From<MemoryStoreError> for RateLimitError {
    fn from(err: MemoryStoreError) -> Self {
        RateLimitError::Policy("memory_store_error".into(), err.to_string())
    }
}

#[derive(Clone, Debug)]
struct Entry {
    used: u64,
    expires_at: Instant,
}

#[derive(Clone, Copy, Debug)]
struct EntryExpiry;

impl Expiry<String, Entry> for EntryExpiry {
    /// Returns the duration until the entry expires.
    fn expire_after_create(&self, _key: &String, entry: &Entry, created_at: Instant) -> Option<Duration> {
        Some(entry.expires_at.saturating_duration_since(created_at))
    }

    /// Returns the duration until the entry expires.
    fn expire_after_update(
        &self,
        _key: &String,
        entry: &Entry,
        updated_at: Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        Some(entry.expires_at.saturating_duration_since(updated_at))
    }
}

/// In-process Fixed Window Store whose clones share one counter set.
///
/// Entries expire with their fixed windows. Window durations use [`Duration`] and [`Instant`]
/// directly, without reducing their precision before storage.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
/// use tower_rate_limiter::MemoryStore;
///
/// let store = MemoryStore::new();
/// ```
#[derive(Clone, Debug)]
pub struct MemoryStore {
    cache: Cache<String, Entry>,
}

impl Default for MemoryStore {
    /// Creates a new MemoryStore with default settings.
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    /// Creates a new MemoryStore.
    pub fn new() -> Self {
        Self {
            cache: Cache::builder().expire_after(EntryExpiry).build(),
        }
    }
}

impl FixedWindowStore for MemoryStore {
    type Future = Ready<Result<FixedWindowUsage, RateLimitError>>;

    fn increment(&self, key: &str, window: Duration) -> Self::Future {
        ready(self.increment_usage(key, window).map_err(Into::into))
    }
}

impl MemoryStore {
    /// Increments the usage counter for the given key.
    ///
    /// Resets the counter when the fixed window expires.
    ///
    /// # Errors
    /// Returns an error if the instant is out of range or the cache computation returns no entry.
    fn increment_usage(&self, key: &str, window: Duration) -> Result<FixedWindowUsage, MemoryStoreError> {
        let entry = self
            .cache
            .entry_by_ref(key)
            .and_try_compute_with(|entry| {
                let now = Instant::now();
                let mut entry = match entry {
                    Some(entry) => entry.into_value(),
                    None => Entry {
                        used: 0,
                        expires_at: now.checked_add(window).ok_or(MemoryStoreError::InstantOutOfRange)?,
                    },
                };

                if now >= entry.expires_at {
                    entry.used = 0;
                    entry.expires_at = now.checked_add(window).ok_or(MemoryStoreError::InstantOutOfRange)?;
                }

                entry.used = entry.used.saturating_add(1);
                Ok(Op::Put(entry))
            })?
            .into_entry()
            .map(|entry| entry.into_value())
            .ok_or(MemoryStoreError::FailedToCompute)?;

        let now = Instant::now();

        Ok(FixedWindowUsage {
            used: entry.used,
            reset_after: entry.expires_at.saturating_duration_since(now),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Arc, thread, time::Duration};

    #[test]
    fn test_increment_usage() {
        let key = "user1";
        let store = MemoryStore::new();

        let window = Duration::from_secs(60);

        let usage = store.increment_usage(key, window).unwrap();

        assert_eq!(usage.used, 1);

        let usage = store.increment_usage(key, window).unwrap();

        assert_eq!(usage.used, 2);
    }

    #[test]
    fn test_fixed_window_reset() {
        let store = MemoryStore::new();
        let key = "user1";
        let window = Duration::from_millis(20);

        let usage = store.increment_usage(key, window).unwrap();

        assert_eq!(usage.used, 1);

        let usage = store.increment_usage(key, window).unwrap();

        assert_eq!(usage.used, 2);

        // wait until window expired
        thread::sleep(Duration::from_millis(30));

        let usage = store.increment_usage(key, window).unwrap();

        assert_eq!(usage.used, 1);
    }

    #[test]
    fn test_clone_share_cache() {
        let store = MemoryStore::new();

        let cloned = store.clone();

        let window = Duration::from_secs(60);

        let usage = store.increment_usage("user1", window).unwrap();

        assert_eq!(usage.used, 1);

        let usage = cloned.increment_usage("user1", window).unwrap();

        assert_eq!(usage.used, 2);
    }

    #[test]
    fn test_concurrent_increment() {
        let store = Arc::new(MemoryStore::new());

        let mut handles = Vec::new();

        for _ in 0..10 {
            let store = store.clone();

            handles.push(thread::spawn(move || {
                store.increment_usage("user1", Duration::from_secs(60)).unwrap();
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let usage = store.increment_usage("user1", Duration::from_secs(60)).unwrap();

        assert_eq!(usage.used, 11);
    }

    #[test]
    fn new_window_starts_after_waiting_for_the_same_key_update() {
        use std::sync::mpsc;

        let store = MemoryStore::new();
        let key = "contended-expiring";
        let initial_window = Duration::from_millis(300);
        let new_window = Duration::from_millis(500);

        assert_eq!(store.increment_usage(key, initial_window).unwrap().used, 1);

        let blocker = store.clone();
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let holding_key = thread::spawn(move || {
            blocker.cache.entry_by_ref(key).and_upsert_with(|entry| {
                locked_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                entry.expect("seeded entry").into_value()
            });
        });

        locked_rx.recv().unwrap();
        let incrementing_store = store.clone();
        let incrementing = thread::spawn(move || incrementing_store.increment_usage(key, new_window).unwrap());

        thread::sleep(Duration::from_millis(400));
        release_tx.send(()).unwrap();

        holding_key.join().unwrap();
        let usage = incrementing.join().unwrap();

        assert_eq!(usage.used, 1);
        assert!(
            usage.reset_after > Duration::from_millis(400),
            "new window was shortened while waiting: {usage:?}"
        );

        thread::sleep(Duration::from_millis(200));
        let next_usage = store.increment_usage(key, new_window).unwrap();
        assert_eq!(
            next_usage.used, 2,
            "new window expired before its requested duration: {next_usage:?}"
        );
    }

    #[test]
    fn fixed_window_expiry_removes_the_cached_entry() {
        let store = MemoryStore::new();

        let window = Duration::from_millis(20);

        assert_eq!(store.increment_usage("user-1", window).unwrap().used, 1);
        thread::sleep(Duration::from_millis(30));

        assert!(!store.cache.contains_key("user-1"));
        let deadline = Instant::now() + Duration::from_secs(2);
        while store.cache.entry_count() != 0 && Instant::now() < deadline {
            store.cache.run_pending_tasks();
            thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(store.cache.entry_count(), 0);
    }
}
