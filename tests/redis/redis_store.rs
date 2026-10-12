use std::{env, future::Future, time::Duration};

use tower_rate_limiter::{FixedWindowStore, RedisStore};

const KEY: &str = "opaque:policy:window:client";
const CONCURRENT_REQUESTS: usize = 16;

async fn test_store(runtime: &str, test_name: &str) -> RedisStore {
    let url = env::var("REDIS_URL").expect("REDIS_URL must point to the test Redis server");
    let client = redis::Client::open(url).expect("valid Redis URL");
    let connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect to test Redis");

    RedisStore::new(connection).with_namespace(format!(
        "tower-rate-limiter-{runtime}-test:{test_name}:{}:{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after Unix epoch")
            .as_nanos()
    ))
}

pub(crate) async fn increments_share_one_fixed_window<S, Sleep>(runtime: &str, sleep: S)
where
    S: FnOnce(Duration) -> Sleep,
    Sleep: Future<Output = ()>,
{
    let store = test_store(runtime, "fixed-window").await;
    let window = Duration::from_secs(5);

    let first = store.increment(KEY, window).await.expect("first increment");
    sleep(Duration::from_millis(50)).await;
    let second = store.increment(KEY, window).await.expect("second increment");

    assert_eq!(first.used, 1);
    assert_eq!(second.used, 2);
    assert!(second.reset_after < first.reset_after);
}

pub(crate) async fn counter_resets_after_window_expires<S, Sleep>(runtime: &str, sleep: S)
where
    S: FnOnce(Duration) -> Sleep,
    Sleep: Future<Output = ()>,
{
    let store = test_store(runtime, "window-expiration").await;
    let window = Duration::from_millis(200);

    let first = store.increment(KEY, window).await.expect("first increment");
    assert_eq!(first.used, 1);

    sleep(Duration::from_millis(300)).await;

    let after_expiration = store.increment(KEY, window).await.expect("increment after expiration");
    assert_eq!(after_expiration.used, 1);
}

pub(crate) async fn concurrent_increments_receive_unique_counts<Run, Runs>(runtime: &str, run: Run)
where
    Run: FnOnce(RedisStore, &'static str, Duration, usize) -> Runs,
    Runs: Future<Output = Vec<u64>>,
{
    let store = test_store(runtime, "concurrent-increments").await;
    let mut counts = run(store, KEY, Duration::from_secs(5), CONCURRENT_REQUESTS).await;
    counts.sort_unstable();

    assert_eq!(counts, (1_u64..=CONCURRENT_REQUESTS as u64).collect::<Vec<_>>());
}
