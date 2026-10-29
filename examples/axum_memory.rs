use axum::{Router, extract::ConnectInfo, routing::get};
use http::Request;
use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::Arc;
use std::{error::Error, net::SocketAddr, time::Duration};
use tower_rate_limiter::{FixedWindow, IpKeyExtractor, MemoryStore, RateLimitLayer};

// check if the request is from an allowlisted IP address
fn is_allowlisted(request: &Request<()>, allowlist: &HashSet<IpAddr>) -> bool {
    request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .is_some_and(|addr| allowlist.contains(&addr.ip()))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let ip = "127.0.0.1".parse::<IpAddr>().unwrap();
    let allowlist = Arc::new(HashSet::from([ip]));

    let key_extractor = IpKeyExtractor::new();
    let global_limiter = RateLimitLayer::builder(key_extractor)
        .policy_name("global-limit")
        .with_policy(FixedWindow::new(MemoryStore::new(), 10, Duration::from_secs(60))?)
        .build()?;

    let auth_allowlist = Arc::clone(&allowlist);
    let auth_limiter = RateLimitLayer::builder(key_extractor)
        .policy_name("auth-limit")
        .skip(move |request| is_allowlisted(request, &auth_allowlist))
        .with_policy(FixedWindow::new(MemoryStore::new(), 3, Duration::from_secs(60))?)
        .build()?;

    let auth_routes = Router::new()
        .route("/login", get(|| async { "login" }))
        .layer(auth_limiter);

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .nest("/auth", auth_routes)
        .layer(global_limiter);

    let address: SocketAddr = "127.0.0.1:3000".parse()?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("listening on http://{address}");
    // ANCHOR: serve
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await?;
    // ANCHOR_END: serve
    Ok(())
}
