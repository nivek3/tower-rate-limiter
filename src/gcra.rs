//! Backend-independent GCRA Quota configuration.

use std::time::Duration;

const MAX_WINDOW: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// Errors returned while validating a static GCRA Quota.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum GcraQuotaError {
    /// A Quota must replenish at least one request per period.
    #[error("GCRA rate must be greater than zero")]
    ZeroRate,

    /// A Quota period must advance time.
    #[error("GCRA period must be greater than zero")]
    ZeroPeriod,

    /// Burst is the total instantaneous capacity and must contain at least one request.
    #[error("GCRA burst must be greater than zero")]
    ZeroBurst,

    /// The time required to replenish the configured burst exceeds the common one-year safety
    /// limit of the built-in GCRA adapters.
    #[error("GCRA burst replenishment window exceeds the one-year safety limit")]
    WindowTooLarge,
}

/// One static GCRA Quota usable by any built-in GCRA Policy adapter.
///
/// `rate` requests are replenished over `period`. `burst` is the total number of requests that a
/// fresh Client Key may consume immediately, not an amount added on top of `rate`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GcraQuota {
    rate: u32,
    period: Duration,
    burst: u32,
    emission_interval: Duration,
    window: Duration,
}

impl GcraQuota {
    /// Construct a validated static Quota.
    pub fn new(rate: u32, period: Duration, burst: u32) -> Result<Self, GcraQuotaError> {
        if rate == 0 {
            return Err(GcraQuotaError::ZeroRate);
        }
        if period.is_zero() {
            return Err(GcraQuotaError::ZeroPeriod);
        }
        if burst == 0 {
            return Err(GcraQuotaError::ZeroBurst);
        }

        let period_nanos = period.as_nanos();
        // All built-in remote adapters exchange whole microseconds. Round the common emission
        // interval up so no adapter admits faster than the configured long-term rate.
        let rate_u128 = u128::from(rate);
        let emission_micros = period_nanos
            .checked_add(rate_u128 * 1_000 - 1)
            .ok_or(GcraQuotaError::WindowTooLarge)?
            / (rate_u128 * 1_000);
        let emission_interval =
            Duration::from_micros(u64::try_from(emission_micros).map_err(|_| GcraQuotaError::WindowTooLarge)?);
        let window = emission_interval
            .checked_mul(burst)
            .ok_or(GcraQuotaError::WindowTooLarge)?;
        if window > MAX_WINDOW {
            return Err(GcraQuotaError::WindowTooLarge);
        }

        Ok(Self {
            rate,
            period,
            burst,
            emission_interval,
            window,
        })
    }

    /// Return the number of requests replenished over the configured period.
    pub const fn rate(&self) -> u32 {
        self.rate
    }

    /// Return the period over which `rate` requests are replenished.
    pub const fn period(&self) -> Duration {
        self.period
    }

    /// Return the total instantaneous capacity.
    pub const fn burst(&self) -> u32 {
        self.burst
    }

    /// Return the time required to replenish one request, rounded up to whole microseconds.
    pub const fn emission_interval(&self) -> Duration {
        self.emission_interval
    }

    /// Return the advertised time required to replenish a complete burst.
    pub const fn window(&self) -> Duration {
        self.window
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rounds_the_common_interval_up_to_microseconds() {
        let quota = GcraQuota::new(3, Duration::from_secs(1), 2).expect("valid quota");

        assert_eq!(quota.emission_interval(), Duration::from_micros(333_334));
        assert_eq!(quota.window(), Duration::from_micros(666_668));
    }
}
