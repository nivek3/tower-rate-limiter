#![cfg(all(feature = "redis-gcra", feature = "runtime-tokio"))]

mod redis_gcra;

use tower_rate_limiter::RateLimitPolicy;

static TEST_SERIALIZATION: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn burst_rejection_does_not_add_penalty_and_recovers() {
    let _serial = TEST_SERIALIZATION.lock().await;
    redis_gcra::burst_rejection_does_not_add_penalty_and_recovers("tokio", tokio::time::sleep).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn separate_connections_share_one_quota() {
    let _serial = TEST_SERIALIZATION.lock().await;
    redis_gcra::separate_connections_share_one_quota("tokio").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_keys_are_isolated() {
    let _serial = TEST_SERIALIZATION.lock().await;
    redis_gcra::client_keys_are_isolated("tokio").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_fresh_charges_allow_exactly_the_burst() {
    let _serial = TEST_SERIALIZATION.lock().await;
    redis_gcra::concurrent_fresh_charges_allow_exactly_the_burst("tokio", |policy, requests| async move {
        let checks = (0..requests)
            .map(|_| {
                let policy = policy.clone();
                tokio::spawn(async move {
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
            allowed += usize::from(check.await.expect("join concurrent charge"));
        }
        allowed
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn script_flush_recovers_on_the_next_charge() {
    let _serial = TEST_SERIALIZATION.lock().await;
    redis_gcra::script_flush_recovers_on_the_next_charge("tokio").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_redis_state_is_a_policy_error() {
    let _serial = TEST_SERIALIZATION.lock().await;
    redis_gcra::corrupt_redis_state_is_a_policy_error("tokio").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_policy_errors_follow_layer_failure_mode() {
    let _serial = TEST_SERIALIZATION.lock().await;
    redis_gcra::redis_policy_errors_follow_layer_failure_mode("tokio").await;
}
