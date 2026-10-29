use std::time::Duration;

use tower_rate_limiter::{GcraQuota, POSTGRES_GCRA_MIGRATION_V1, PostgresGcra, RateLimitPolicy};

#[test]
fn postgres_gcra_is_a_public_rate_limit_policy() {
    fn assert_policy<P: RateLimitPolicy>() {}

    assert_policy::<PostgresGcra>();
}

#[test]
fn postgres_gcra_keeps_the_shared_quota_shape() {
    let quota = GcraQuota::new(10, Duration::from_secs(1), 4).expect("valid GCRA quota");
    assert_eq!(quota.burst(), 4);
    assert_eq!(quota.emission_interval(), Duration::from_millis(100));
}

#[test]
fn versioned_postgres_migration_is_available_to_application_tooling() {
    assert!(POSTGRES_GCRA_MIGRATION_V1.contains("tower_rate_limiter_gcra_v1.charge"));
    assert!(POSTGRES_GCRA_MIGRATION_V1.contains("REVOKE ALL ON FUNCTION"));
}
