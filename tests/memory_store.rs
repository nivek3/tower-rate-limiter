#![cfg(feature = "memory")]

use std::{sync::Arc, thread, time::Duration};

use tower_rate_limiter::{FixedWindowStore, FixedWindowUsage, MemoryStore};

fn increment(store: &MemoryStore, key: &str, window: Duration) -> FixedWindowUsage {
    store
        .increment(key, window)
        .into_inner()
        .expect("memory store increment")
}

#[test]
fn active_window_ignores_later_window_arguments() {
    let store = MemoryStore::new();
    let first = increment(&store, "key", Duration::from_secs(1));

    thread::sleep(Duration::from_millis(50));

    // A larger window must not extend the active window.
    let second = increment(&store, "key", Duration::from_secs(10));

    assert_eq!(first.used, 1);
    assert_eq!(second.used, 2);
    assert!(first.reset_after > Duration::ZERO);
    assert!(second.reset_after < first.reset_after);

    // A shorter window must not shorten/reset the active window either.
    let third = increment(&store, "key", Duration::from_millis(10));

    assert_eq!(third.used, 3);
    assert!(third.reset_after <= second.reset_after);

    // Wait longer than the later 10ms window.
    // The original 1s fixed window should still be active.
    thread::sleep(Duration::from_millis(20));

    let fourth = increment(&store, "key", Duration::from_secs(1));

    assert_eq!(fourth.used, 4);
}

#[test]
fn independent_keys_do_not_share_usage() {
    let store = MemoryStore::new();

    assert_eq!(increment(&store, "a", Duration::from_secs(60)).used, 1);
    assert_eq!(increment(&store, "a", Duration::from_secs(60)).used, 2);
    assert_eq!(increment(&store, "b", Duration::from_secs(60)).used, 1);
}

#[test]
fn clones_share_usage() {
    let store = MemoryStore::new();
    let clone = store.clone();

    assert_eq!(increment(&store, "shared", Duration::from_secs(60)).used, 1);
    assert_eq!(increment(&clone, "shared", Duration::from_secs(60)).used, 2);
}

#[test]
fn concurrent_increments_are_not_lost() {
    let store = Arc::new(MemoryStore::new());
    let concurrency = 100;

    thread::scope(|scope| {
        let handles = (0..concurrency)
            .map(|_| {
                let store = Arc::clone(&store);
                scope.spawn(move || increment(&store, "concurrent", Duration::from_secs(60)))
            })
            .collect::<Vec<_>>();

        let usages = handles
            .into_iter()
            .map(|handle| handle.join().expect("increment thread"))
            .collect::<Vec<_>>();
        let final_usage = increment(&store, "concurrent", Duration::from_secs(60));

        assert_eq!(final_usage.used, concurrency + 1);
        assert!(usages.iter().all(|usage| final_usage.reset_after <= usage.reset_after));
    });
}

#[test]
fn expired_usage_restarts_at_one_with_a_new_window() {
    let store = MemoryStore::new();
    let key = "expiring";

    let first = increment(&store, key, Duration::from_millis(10));

    assert_eq!(first.used, 1);
    assert!(first.reset_after > Duration::ZERO);

    // Wait until the first fixed window has definitely expired.
    thread::sleep(Duration::from_millis(20));

    // Start a new window with a different duration.
    let second_window = Duration::from_secs(1);

    let restarted = increment(&store, key, second_window);

    // The expired entry must restart from 1.
    assert_eq!(restarted.used, 1);

    // The new window should use the newly provided duration,
    // rather than keeping the old 10ms window.
    assert!(restarted.reset_after > Duration::from_millis(500));
    assert!(restarted.reset_after <= second_window);

    // Another increment inside the new window should continue
    // from the restarted counter.
    let second = increment(&store, key, second_window);

    assert_eq!(second.used, 2);
    assert!(second.reset_after <= restarted.reset_after);
}

#[test]
fn different_scopes_remain_independent_in_one_store() {
    let store = MemoryStore::new();

    assert_eq!(increment(&store, "policy-a-window-60", Duration::from_secs(60)).used, 1);
    assert_eq!(increment(&store, "policy-b-window-60", Duration::from_secs(60)).used, 1);
    assert_eq!(increment(&store, "policy-a-window-30", Duration::from_secs(30)).used, 1);
}
