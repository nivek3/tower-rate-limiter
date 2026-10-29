#![cfg(all(feature = "redis", feature = "runtime-smol"))]

mod redis_store;

use tower_rate_limiter::FixedWindowStore;

#[test]
fn increments_share_one_fixed_window() {
    smol::block_on(redis_store::increments_share_one_fixed_window(
        "smol",
        |duration| async move {
            smol::Timer::after(duration).await;
        },
    ));
}

#[test]
fn counter_resets_after_window_expires() {
    smol::block_on(redis_store::counter_resets_after_window_expires(
        "smol",
        |duration| async move {
            smol::Timer::after(duration).await;
        },
    ));
}

#[test]
fn concurrent_increments_receive_unique_counts() {
    smol::block_on(redis_store::concurrent_increments_receive_unique_counts(
        "smol",
        |store, key, window, request_count| async move {
            let increments = (0..request_count)
                .map(|_| {
                    let store = store.clone();
                    smol::spawn(async move { store.increment(key, window).await.expect("concurrent increment").used })
                })
                .collect::<Vec<_>>();

            let mut counts = Vec::with_capacity(increments.len());
            for increment in increments {
                counts.push(increment.await);
            }
            counts
        },
    ));
}
