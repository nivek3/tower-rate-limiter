#![cfg(all(feature = "redis", feature = "runtime-tokio"))]

mod redis_store;

use tower_rate_limiter::FixedWindowStore;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn increments_share_one_fixed_window() {
    redis_store::increments_share_one_fixed_window("tokio", tokio::time::sleep).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn counter_resets_after_window_expires() {
    redis_store::counter_resets_after_window_expires("tokio", tokio::time::sleep).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_increments_receive_unique_counts() {
    redis_store::concurrent_increments_receive_unique_counts("tokio", |store, key, window, request_count| async move {
        let increments = (0..request_count)
            .map(|_| {
                let store = store.clone();
                tokio::spawn(async move { store.increment(key, window).await.expect("concurrent increment").used })
            })
            .collect::<Vec<_>>();

        let mut counts = Vec::with_capacity(increments.len());
        for increment in increments {
            counts.push(increment.await.expect("join increment task"));
        }
        counts
    })
    .await;
}
