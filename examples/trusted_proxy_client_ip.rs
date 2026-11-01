use std::{
    error::Error,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use axum::{Router, routing::get};
use tower_rate_limiter::{FixedWindow, MemoryStore, RateLimitLayer, TrustedProxyClientIpKeyExtractor};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Replace this documentation address with application-owned proxy configuration. The proxy
    // must remove or overwrite every supported client-IP Header, and ingress must prevent clients
    // from bypassing it through an address accepted by this policy.
    let trusted_proxy: IpAddr = "192.0.2.10".parse()?;
    let key_extractor = TrustedProxyClientIpKeyExtractor::new(move |peer| peer == trusted_proxy);
    let limiter = RateLimitLayer::builder(key_extractor)
        .policy_name("client-ip-limit")
        .with_policy(FixedWindow::new(MemoryStore::new(), 100, Duration::from_secs(60))?)
        .build()?;
    let app = Router::new().route("/health", get(|| async { "ok" })).layer(limiter);

    let address: SocketAddr = "127.0.0.1:3001".parse()?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("listening on http://{address}");
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await?;
    Ok(())
}
