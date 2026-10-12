use std::{env, error::Error, net::SocketAddr, time::Duration};

use axum::{Router, http::StatusCode, routing::get};
use http::Request;
use tower_rate_limiter::{FixedWindow, KeyExtractor, MemoryStore, RateLimitError, RateLimitLayer, RedisStore};

const DEFAULT_ADDRESS: &str = "127.0.0.1:3000";
const DEFAULT_LIMIT: u64 = 1_000_000_000;
const DEFAULT_WINDOW_SECS: u64 = 3_600;
const POLICY_NAME: &str = "default-policy";

#[cfg(feature = "redis-lua")]
const REDIS_IMPLEMENTATION: &str = "lua";
#[cfg(not(feature = "redis-lua"))]
const REDIS_IMPLEMENTATION: &str = "transaction";

#[derive(Clone, Copy)]
struct BenchmarkKeyExtractor;

impl KeyExtractor for BenchmarkKeyExtractor {
    type Key = String;

    fn extract<B>(&self, request: &Request<B>) -> Result<Self::Key, RateLimitError> {
        match request.headers().get("x-bench-key") {
            Some(value) => value.to_str().map(str::to_owned).map_err(|_| {
                RateLimitError::Key(
                    String::from("invalid_benchmark_key"),
                    String::from("x-bench-key must be valid UTF-8"),
                )
            }),
            None => Ok(String::from("hot")),
        }
    }
}

async fn empty_response() -> StatusCode {
    StatusCode::NO_CONTENT
}

fn u64_from_env(name: &str, default: u64) -> Result<u64, Box<dyn Error>> {
    match env::var(name) {
        Ok(value) => Ok(value.parse::<u64>()?),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();

    let address = env::var("BENCH_ADDRESS")
        .unwrap_or_else(|_| String::from(DEFAULT_ADDRESS))
        .parse::<SocketAddr>()?;
    let limit = u64_from_env("BENCH_LIMIT", DEFAULT_LIMIT)?;
    let window = Duration::from_secs(u64_from_env("BENCH_WINDOW_SECS", DEFAULT_WINDOW_SECS)?);

    let redis_url = env::var("REDIS_URL").expect("REDIS_URL must be set");
    let redis_client = redis::Client::open(redis_url)?;
    let redis_connection = redis_client.get_multiplexed_async_connection().await?;
    let redis_namespace =
        env::var("BENCH_REDIS_NAMESPACE").unwrap_or_else(|_| format!("benchmark-{REDIS_IMPLEMENTATION}"));

    let benchmark_info = format!(
        "redis_benchmark={REDIS_IMPLEMENTATION}\npolicy_name={POLICY_NAME}\nlimit={limit}\nwindow_secs={}\nredis_namespace={redis_namespace}\n",
        window.as_secs()
    );

    let memory_limiter = RateLimitLayer::builder(BenchmarkKeyExtractor)
        .policy_name(POLICY_NAME)
        .with_policy(FixedWindow::new(MemoryStore::new(), limit, window)?)
        .build()?;
    let redis_limiter = RateLimitLayer::builder(BenchmarkKeyExtractor)
        .policy_name(POLICY_NAME)
        .with_policy(FixedWindow::new(
            RedisStore::new(redis_connection).with_namespace(redis_namespace),
            limit,
            window,
        )?)
        .build()?;

    let app = Router::new()
        .route(
            "/info",
            get(move || {
                let benchmark_info = benchmark_info.clone();
                async move { benchmark_info }
            }),
        )
        .route("/baseline", get(empty_response))
        .merge(
            Router::new()
                .route("/memory", get(empty_response))
                .layer(memory_limiter),
        )
        .merge(Router::new().route("/redis", get(empty_response)).layer(redis_limiter));

    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("benchmark server listening on http://{address}");
    println!("Redis implementation: {REDIS_IMPLEMENTATION}");
    println!("limit: {limit}, window: {}s", window.as_secs());
    println!("routes: /info, /baseline, /memory, /redis");
    axum::serve(listener, app).await?;
    Ok(())
}
