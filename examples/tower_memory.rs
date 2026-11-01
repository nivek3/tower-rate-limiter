use std::{convert::Infallible, error::Error, future::ready, time::Duration};

use http::{Request, Response};
use tower::{Layer, service_fn};
use tower_rate_limiter::{FixedWindow, KeyExtractor, MemoryStore, RateLimitLayer};

#[derive(Clone, Copy)]
struct StaticClient;

impl KeyExtractor for StaticClient {
    type Key = String;

    fn extract<B>(&self, _request: &Request<B>) -> Result<Self::Key, tower_rate_limiter::RateLimitError> {
        Ok(String::from("example-client"))
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();

    let limiter = RateLimitLayer::builder(StaticClient)
        .policy_name("tower-example")
        .with_policy(FixedWindow::new(MemoryStore::new(), 100, Duration::from_secs(60))?)
        .build()?;

    let _service = limiter.layer(service_fn(|_request: Request<()>| {
        ready(Ok::<Response<()>, Infallible>(Response::new(())))
    }));

    println!("constructed a Tower MemoryStore rate limiter");
    Ok(())
}
