#![cfg(all(feature = "redis-gcra", feature = "runtime-smol"))]

mod redis_gcra;

use std::sync::{LazyLock, Mutex, MutexGuard};

use tower_rate_limiter::RateLimitPolicy;

static TEST_SERIALIZATION: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn serial_guard() -> MutexGuard<'static, ()> {
    TEST_SERIALIZATION
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[test]
fn burst_rejection_does_not_add_penalty_and_recovers() {
    let _serial = serial_guard();
    smol::block_on(redis_gcra::burst_rejection_does_not_add_penalty_and_recovers(
        "smol",
        |duration| async move {
            smol::Timer::after(duration).await;
        },
    ));
}

#[test]
fn separate_connections_share_one_quota() {
    let _serial = serial_guard();
    smol::block_on(redis_gcra::separate_connections_share_one_quota("smol"));
}

#[test]
fn client_keys_are_isolated() {
    let _serial = serial_guard();
    smol::block_on(redis_gcra::client_keys_are_isolated("smol"));
}

#[test]
fn concurrent_fresh_charges_allow_exactly_the_burst() {
    let _serial = serial_guard();
    smol::block_on(redis_gcra::concurrent_fresh_charges_allow_exactly_the_burst(
        "smol",
        |policy, requests| async move {
            let checks = (0..requests)
                .map(|_| {
                    let policy = policy.clone();
                    smol::spawn(async move {
                        !policy
                            .check("concurrent-client".into())
                            .await
                            .expect("concurrent GCRA charge")
                            .is_rate_limited()
                    })
                })
                .collect::<Vec<_>>();

            let mut allowed = 0;
            for check in checks {
                allowed += usize::from(check.await);
            }
            allowed
        },
    ));
}

#[test]
fn script_flush_recovers_on_the_next_charge() {
    let _serial = serial_guard();
    smol::block_on(redis_gcra::script_flush_recovers_on_the_next_charge("smol"));
}

#[test]
fn corrupt_redis_state_is_a_policy_error() {
    let _serial = serial_guard();
    smol::block_on(redis_gcra::corrupt_redis_state_is_a_policy_error("smol"));
}

#[test]
fn redis_policy_errors_follow_layer_failure_mode() {
    let _serial = serial_guard();
    smol::block_on(redis_gcra::redis_policy_errors_follow_layer_failure_mode("smol"));
}
