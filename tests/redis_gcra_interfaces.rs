use std::time::Duration;

use tower_rate_limiter::{GcraQuota, GcraQuotaError, RateLimitPolicy, RedisGcra};

#[test]
fn gcra_quota_exposes_the_validated_static_configuration() {
    let quota = GcraQuota::new(10, Duration::from_secs(1), 4).expect("valid GCRA quota");

    assert_eq!(quota.rate(), 10);
    assert_eq!(quota.period(), Duration::from_secs(1));
    assert_eq!(quota.burst(), 4);
    assert_eq!(quota.emission_interval(), Duration::from_millis(100));
    assert_eq!(quota.window(), Duration::from_millis(400));
}

#[test]
fn gcra_quota_validates_and_rounds_the_common_backend_configuration() {
    assert_eq!(
        GcraQuota::new(0, Duration::from_secs(1), 1),
        Err(GcraQuotaError::ZeroRate)
    );
    assert_eq!(GcraQuota::new(1, Duration::ZERO, 1), Err(GcraQuotaError::ZeroPeriod));
    assert_eq!(
        GcraQuota::new(1, Duration::from_secs(1), 0),
        Err(GcraQuotaError::ZeroBurst)
    );
    let sub_microsecond = GcraQuota::new(2, Duration::from_micros(1), 1).expect("sub-microsecond interval rounds up");
    assert_eq!(sub_microsecond.emission_interval(), Duration::from_micros(1));
    assert_eq!(
        GcraQuota::new(1, Duration::from_secs(365 * 24 * 60 * 60 + 1), 1),
        Err(GcraQuotaError::WindowTooLarge)
    );
    GcraQuota::new(1, Duration::from_secs(365 * 24 * 60 * 60), 1).expect("one-year replenishment window is supported");

    let minimum =
        GcraQuota::new(1_000, Duration::from_millis(1), 1).expect("one-microsecond emission interval is supported");
    assert_eq!(minimum.emission_interval(), Duration::from_micros(1));
}

#[test]
fn redis_gcra_is_a_public_rate_limit_policy() {
    fn assert_policy<P: RateLimitPolicy>() {}

    assert_policy::<RedisGcra>();
}
