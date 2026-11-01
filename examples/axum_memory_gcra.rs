use std::{error::Error, net::SocketAddr, time::Duration};

use axum::{Router, routing::get};
use tower_rate_limiter::{ConfigError, GcraQuota, IpKeyExtractor, MemoryGcra, RateLimitLayer};

fn app(policy: MemoryGcra) -> Result<Router, ConfigError> {
    let limiter = RateLimitLayer::builder(IpKeyExtractor::new())
        .policy_name("public-api")
        .with_policy(policy)
        .build()?;

    Ok(Router::new().route("/", get(|| async { "hello" })).layer(limiter))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let quota = GcraQuota::new(10, Duration::from_secs(60), 3)?;
    let policy = MemoryGcra::new(quota);
    let app = app(policy.clone())?;
    let address: SocketAddr = "127.0.0.1:3000".parse()?;
    let listener = tokio::net::TcpListener::bind(address).await?;

    // The application owns cleanup; clones share the same keyed state.
    let cleanup = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            policy.prune_inactive_keys();
        }
    });

    println!("listening on http://{address}");
    let result = axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await;
    cleanup.abort();
    result?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::{body::Body, extract::ConnectInfo};
    use http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn peer_ip_is_limited_and_receives_retry_after() {
        let quota = GcraQuota::new(1, Duration::from_secs(60), 1).expect("valid quota");
        let app = app(MemoryGcra::new(quota)).expect("valid app");
        let request = || {
            let mut request = Request::new(Body::empty());
            request
                .extensions_mut()
                .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 12345))));
            request
        };

        let allowed = app.clone().oneshot(request()).await.expect("allowed response");
        assert_eq!(allowed.status(), StatusCode::OK);
        let rejected = app.oneshot(request()).await.expect("rejected response");
        assert_eq!(rejected.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            rejected.headers()["retry-after"]
                .to_str()
                .expect("valid retry header")
                .parse::<u64>()
                .expect("retry seconds")
                > 0
        );
    }
}
