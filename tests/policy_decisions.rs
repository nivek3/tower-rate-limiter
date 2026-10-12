use std::{
    convert::Infallible,
    future::{Ready, ready},
    task::{Context, Poll},
    time::Duration,
};

use http::{Request, Response, StatusCode};
use tower::{Layer, Service, ServiceExt};
use tower_rate_limiter::{Decision, KeyExtractor, RateLimitError, RateLimitLayer, RateLimitPolicy};

#[derive(Clone, Copy)]
struct StaticKey;

impl KeyExtractor for StaticKey {
    type Key = &'static str;

    fn extract<B>(&self, _request: &Request<B>) -> Result<Self::Key, RateLimitError> {
        Ok("caller")
    }
}

#[derive(Clone, Copy)]
struct AlwaysAllow;

impl RateLimitPolicy for AlwaysAllow {
    type Future = Ready<Result<Decision, RateLimitError>>;

    fn check(&self, key: String) -> Self::Future {
        assert_eq!(key, "api:caller");
        ready(Ok(Decision::allowed(
            2,
            Duration::from_secs(60),
            1,
            Duration::from_secs(45),
        )))
    }
}

#[derive(Clone, Copy)]
struct OkService;

impl Service<Request<()>> for OkService {
    type Response = Response<()>;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: Request<()>) -> Self::Future {
        ready(Ok(Response::new(())))
    }
}

#[test]
fn custom_policy_decision_drives_the_layer_response() {
    let layer = RateLimitLayer::builder(StaticKey)
        .policy_name("api")
        .with_policy(AlwaysAllow)
        .build()
        .expect("valid layer");

    let response = smol::block_on(layer.layer(OkService).oneshot(Request::new(()))).expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["ratelimit-policy"], "\"api\";q=2;w=60");
    assert_eq!(response.headers()["ratelimit"], "\"api\";r=1;t=45");
}
