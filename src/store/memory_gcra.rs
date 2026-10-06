//! Process-local GCRA Policy backed by governor.

use std::{
    fmt,
    future::{Ready, ready},
    num::NonZeroU32,
    sync::Arc,
    time::Duration,
};

use governor::{
    DefaultKeyedRateLimiter, RateLimiter,
    clock::Clock,
    middleware::{StateInformationMiddleware, StateSnapshot},
};

use crate::{Decision, GcraQuota, RateLimitError, RateLimitPolicy};

type KeyedLimiter = DefaultKeyedRateLimiter<String, StateInformationMiddleware>;

/// A process-local keyed GCRA Policy delegated to governor.
///
/// Each constructor creates an independent limiter. Cloning a value shares its keyed state, so
/// cloned Policies enforce the same per-key quota within one process. Every check charges exactly
/// one request.
#[derive(Clone)]
pub struct MemoryGcra {
    limiter: Arc<KeyedLimiter>,
    quota: GcraQuota,
}

impl MemoryGcra {
    /// Construct a process-local GCRA Policy with one static Quota per Client Key.
    pub fn new(quota: GcraQuota) -> Self {
        let governor_quota = governor::Quota::with_period(quota.emission_interval())
            .expect("GcraQuota always has a non-zero emission interval")
            .allow_burst(NonZeroU32::new(quota.burst()).expect("GcraQuota always has a non-zero burst"));
        let limiter = RateLimiter::keyed(governor_quota).with_middleware::<StateInformationMiddleware>();

        Self {
            limiter: Arc::new(limiter),
            quota,
        }
    }

    /// Return the static Quota owned by this Policy.
    pub const fn quota(&self) -> GcraQuota {
        self.quota
    }

    /// Remove Client Keys whose rate-limit state is indistinguishable from a fresh key.
    ///
    /// Applications accepting an unbounded key space should call this periodically to keep the
    /// process-local state map bounded.
    pub fn prune_inactive_keys(&self) {
        self.limiter.retain_recent();
    }

    /// Return an approximate count of Client Keys currently tracked by this Policy.
    pub fn tracked_key_count(&self) -> usize {
        self.limiter.len()
    }
}

impl fmt::Debug for MemoryGcra {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryGcra")
            .field("quota", &self.quota)
            .finish_non_exhaustive()
    }
}

impl RateLimitPolicy for MemoryGcra {
    type Future = Ready<Result<Decision, RateLimitError>>;

    fn check(&self, key: String) -> Self::Future {
        let decision = match self.limiter.check_key(&key) {
            Ok(snapshot) => allowed_decision(self.quota, snapshot),
            Err(not_until) => {
                let retry_after = not_until.wait_time_from(self.limiter.clock().now());
                rate_limited_decision(self.quota, retry_after)
            },
        };

        ready(Ok(decision))
    }
}

fn allowed_decision(quota: GcraQuota, snapshot: StateSnapshot) -> Decision {
    let remaining = snapshot.remaining_burst_capacity();
    let missing_capacity = quota.burst().saturating_sub(remaining);
    // The snapshot is captured immediately after the accepted charge. Waiting one emission
    // interval per missing cell is therefore a conservative guarantee that the full burst is
    // available again; elapsed time before the caller receives the Decision only helps.
    let reset_after = duration_times(quota.emission_interval(), missing_capacity);

    Decision::allowed(
        u64::from(quota.burst()),
        quota.window(),
        u64::from(remaining),
        reset_after,
    )
}

fn rate_limited_decision(quota: GcraQuota, retry_after: Duration) -> Decision {
    // Governor's NotUntil is the earliest conforming retry. `tau` is the remaining tolerance of
    // a full GCRA burst, so retry + tau conservatively advertises when full capacity is restored.
    let tau = duration_times(quota.emission_interval(), quota.burst().saturating_sub(1));
    let reset_after = retry_after
        .checked_add(tau)
        .expect("GcraQuota bounds the full replenishment window");

    Decision::rate_limited(u64::from(quota.burst()), quota.window(), reset_after, retry_after)
}

fn duration_times(duration: Duration, multiplier: u32) -> Duration {
    duration
        .checked_mul(multiplier)
        .expect("GcraQuota bounds the full replenishment window")
}
