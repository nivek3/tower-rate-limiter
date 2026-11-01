//! Fixed Window Policy and its private storage seam.

use std::{future::Future, pin::Pin, task::Poll, time::Duration};

use pin_project_lite::pin_project;

use super::{ConfigError, Decision, RateLimitError, RateLimitPolicy};

/// The operating mode to use when a rate-limit Policy fails.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PolicyFailureMode {
    /// Build an error response without calling the inner service.
    #[default]
    Reject,
    /// Call the inner service without rate-limit metadata.
    Allow,
}

/// Usage returned by a [`FixedWindowStore`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedWindowUsage {
    /// Total charged requests in the current window, including this increment.
    pub used: u64,
    /// Time remaining until the current window resets.
    pub reset_after: Duration,
}

/// An asynchronous fixed-window storage adapter.
///
/// Implementations receive a complete, policy-scoped Client Key and must treat it as opaque. For
/// each key, [`FixedWindowStore::increment`] must atomically create or increment one fixed window,
/// start expiry on the first increment, and leave that expiry unchanged on later increments.
/// Clones of one value must observe the same counter state.
pub trait FixedWindowStore: Clone {
    /// The concrete future returned by [`FixedWindowStore::increment`].
    type Future: Future<Output = Result<FixedWindowUsage, RateLimitError>>;

    /// Atomically increment `key` for the fixed `window`.
    fn increment(&self, key: &str, window: Duration) -> Self::Future;
}

/// Fixed Window Policy backed by a [`FixedWindowStore`].
#[derive(Clone, Debug)]
pub struct FixedWindow<S> {
    store: S,
    limit: u64,
    window: Duration,
}

impl<S> FixedWindow<S> {
    /// Construct a Fixed Window Policy.
    pub fn new(store: S, limit: u64, window: Duration) -> Result<Self, ConfigError> {
        const MINIMUM_WINDOW: Duration = Duration::from_millis(1);
        if window < MINIMUM_WINDOW {
            return Err(ConfigError::WindowTooShort(window, MINIMUM_WINDOW));
        }

        Ok(Self { store, limit, window })
    }

    /// Borrow the underlying storage adapter.
    pub const fn store(&self) -> &S {
        &self.store
    }

    /// Return the Policy's fixed request limit.
    pub const fn limit(&self) -> u64 {
        self.limit
    }

    /// Return the Policy's fixed window.
    pub const fn window(&self) -> Duration {
        self.window
    }
}

pin_project! {
    /// Future returned by [`FixedWindow`].
    #[derive(Debug)]
    pub struct FixedWindowFuture<F> {
        #[pin]
        future: F,
        limit: u64,
        window: Duration,
    }
}

impl<F> Future for FixedWindowFuture<F>
where
    F: Future<Output = Result<FixedWindowUsage, RateLimitError>>,
{
    type Output = Result<Decision, RateLimitError>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let usage = match std::task::ready!(this.future.poll(cx)) {
            Ok(usage) => usage,
            Err(error) => return Poll::Ready(Err(error)),
        };

        if usage.used == 0 {
            return Poll::Ready(Err(RateLimitError::Policy(
                String::from("invalid_usage"),
                String::from("fixed-window store returned zero usage"),
            )));
        }

        let decision = if usage.used <= *this.limit {
            Decision::allowed(
                *this.limit,
                *this.window,
                this.limit.saturating_sub(usage.used),
                usage.reset_after,
            )
        } else {
            Decision::rate_limited(*this.limit, *this.window, usage.reset_after, usage.reset_after)
        };

        Poll::Ready(Ok(decision))
    }
}

impl<S> RateLimitPolicy for FixedWindow<S>
where
    S: FixedWindowStore,
{
    type Future = FixedWindowFuture<S::Future>;

    fn check(&self, key: String) -> Self::Future {
        FixedWindowFuture {
            future: self.store.increment(&key, self.window),
            limit: self.limit,
            window: self.window,
        }
    }
}
