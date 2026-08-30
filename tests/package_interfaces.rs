use std::{
    convert::Infallible,
    fmt,
    future::Future,
    future::{Ready, ready},
    net::{IpAddr, SocketAddr},
    pin::{Pin, pin},
    rc::Rc,
    task::{Context, Poll, Waker},
    time::Duration,
};

use http::{Request, Response, header};
use tower::{Layer, Service};
use tower_rate_limiter::{
    ClientIpKeyExtractor, ConfigError, FixedWindow, FixedWindowStore, FixedWindowUsage, IpKeyExtractor, KeyExtractor,
    PolicyFailureMode, RateLimitError, RateLimitLayer, RateLimitPolicy, ResponseFactory, ResponseFuture,
    ResponseReason, TrustedProxyClientIpKeyExtractor,
};

#[derive(Clone, Debug)]
struct StaticKey;

impl KeyExtractor for StaticKey {
    type Key = String;

    fn extract<B>(&self, _request: &Request<B>) -> Result<Self::Key, RateLimitError> {
        Ok(String::from("caller"))
    }
}

#[derive(Clone, Debug)]
struct TestStore;

impl FixedWindowStore for TestStore {
    type Future = Ready<Result<FixedWindowUsage, RateLimitError>>;

    fn increment(&self, _key: &str, window: Duration) -> Self::Future {
        ready(Ok(FixedWindowUsage {
            used: 1,
            reset_after: window,
        }))
    }
}

#[test]
fn response_future_is_publicly_nameable() {
    #[allow(dead_code)]
    fn assert_public<ReqBody, Inner, P, F>()
    where
        Inner: tower::Service<Request<ReqBody>>,
        P: RateLimitPolicy,
    {
        let _: Option<ResponseFuture<ReqBody, Inner, P, F>> = None;
    }
}

struct DisplayOnlyKey;

impl fmt::Display for DisplayOnlyKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("display-only")
    }
}

#[derive(Clone)]
struct LocalKeyExtractor(Rc<()>);

impl KeyExtractor for LocalKeyExtractor {
    type Key = DisplayOnlyKey;

    fn extract<B>(&self, _request: &Request<B>) -> Result<Self::Key, RateLimitError> {
        let _ = &self.0;
        Ok(DisplayOnlyKey)
    }
}

struct LocalFuture<T> {
    value: Option<T>,
    _not_send: Rc<()>,
}

impl<T> LocalFuture<T> {
    fn ready(value: T) -> Self {
        Self {
            value: Some(value),
            _not_send: Rc::new(()),
        }
    }
}

impl<T: Unpin> Future for LocalFuture<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(self.value.take().expect("local future polled after completion"))
    }
}

#[derive(Clone)]
struct LocalStore(Rc<()>);

impl FixedWindowStore for LocalStore {
    type Future = LocalFuture<Result<FixedWindowUsage, RateLimitError>>;

    fn increment(&self, _key: &str, window: Duration) -> Self::Future {
        let _ = &self.0;
        LocalFuture::ready(Ok(FixedWindowUsage {
            used: 1,
            reset_after: window,
        }))
    }
}

#[derive(Clone)]
struct LocalResponseFactory(Rc<()>);

impl ResponseFactory<Rc<()>, Rc<()>> for LocalResponseFactory {
    fn build(&self, _request: Request<Rc<()>>, reason: ResponseReason) -> Response<Rc<()>> {
        let _ = (&self.0, reason);
        Response::new(Rc::new(()))
    }
}

#[derive(Clone)]
struct LocalService(Rc<()>);

impl Service<Request<Rc<()>>> for LocalService {
    type Response = Response<Rc<()>>;
    type Error = Infallible;
    type Future = LocalFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: Request<Rc<()>>) -> Self::Future {
        let _ = &self.0;
        LocalFuture::ready(Ok(Response::new(Rc::new(()))))
    }
}

#[test]
fn local_components_futures_and_display_only_keys_are_supported() {
    let local = Rc::new(());
    let policy = FixedWindow::new(LocalStore(Rc::clone(&local)), 1, Duration::from_secs(1))
        .expect("valid local fixed-window policy");
    let layer = RateLimitLayer::builder(LocalKeyExtractor(Rc::clone(&local)))
        .with_policy(policy)
        .response_factory(LocalResponseFactory(Rc::clone(&local)))
        .build()
        .expect("valid local layer");
    let mut service = layer.layer(LocalService(local));
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(service.poll_ready(&mut context), Poll::Ready(Ok(()))));
    let mut response = pin!(service.call(Request::new(Rc::new(()))));
    assert!(matches!(response.as_mut().poll(&mut context), Poll::Ready(Ok(_))));
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct InnerValue(usize);

#[test]
fn rate_limit_exposes_standard_inner_service_accessors() {
    let policy = FixedWindow::new(TestStore, 7, Duration::from_secs(30)).expect("valid fixed-window policy");
    let layer = RateLimitLayer::builder(StaticKey)
        .with_policy(policy)
        .build()
        .expect("valid layer");
    let mut service = layer.layer(InnerValue(1));

    assert_eq!(service.get_ref(), &InnerValue(1));
    service.get_mut().0 = 2;
    assert_eq!(service.into_inner(), InnerValue(2));
}

#[test]
fn public_composition_types_offer_non_revealing_debug_output() {
    let policy = FixedWindow::new(TestStore, 7, Duration::from_secs(30)).expect("valid fixed-window policy");
    let builder = RateLimitLayer::builder(StaticKey).with_policy(policy);
    assert!(format!("{builder:?}").contains("RateLimitBuilder"));

    let layer = builder.build().expect("valid layer");
    assert!(format!("{layer:?}").contains("RateLimitLayer"));
    assert!(format!("{:?}", layer.layer(InnerValue(1))).contains("RateLimit"));

    let trusted = TrustedProxyClientIpKeyExtractor::new(|_| true);
    let debug = format!("{trusted:?}");
    assert!(debug.contains("TrustedProxyClientIpKeyExtractor"));
    assert!(!debug.contains("closure"));
}

#[test]
fn rate_limit_errors_expose_a_stable_code_and_message() {
    let error = RateLimitError::Policy(
        String::from("redis_unavailable"),
        String::from("usage increment failed"),
    );

    assert!(matches!(
        error,
        RateLimitError::Policy(code, message)
            if code == "redis_unavailable" && message == "usage increment failed"
    ));
}

#[derive(Clone)]
struct TestFactory;

impl ResponseFactory<Vec<u8>, Vec<u8>> for TestFactory {
    fn build(&self, _request: Request<Vec<u8>>, _reason: ResponseReason) -> Response<Vec<u8>> {
        Response::new(Vec::new())
    }
}

#[derive(Clone)]
struct DifferentBodyService;

impl Service<Request<Vec<u8>>> for DifferentBodyService {
    type Response = Response<String>;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: Request<Vec<u8>>) -> Self::Future {
        ready(Ok(Response::new(String::from("inner response"))))
    }
}

#[test]
fn request_and_response_body_types_may_differ() {
    let policy = FixedWindow::new(TestStore, 7, Duration::from_secs(30)).expect("valid fixed-window policy");
    let layer = RateLimitLayer::builder(StaticKey)
        .with_policy(policy)
        .build()
        .expect("valid layer");
    let mut service = layer.layer(DifferentBodyService);
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(service.poll_ready(&mut context), Poll::Ready(Ok(()))));
    let mut response = pin!(service.call(Request::new(Vec::new())));
    let Poll::Ready(Ok(response)) = response.as_mut().poll(&mut context) else {
        panic!("different-body response must be ready");
    };
    assert_eq!(response.body(), "inner response");
}

#[test]
fn custom_store_builder_is_available_without_default_features() {
    let policy = FixedWindow::new(TestStore, 7, Duration::from_secs(30)).expect("valid fixed-window policy");
    let _layer = RateLimitLayer::builder(StaticKey)
        .policy_name("api")
        .policy_failure_mode(PolicyFailureMode::Allow)
        .with_policy(policy)
        .response_factory(TestFactory)
        .build()
        .expect("valid builder configuration");
}

#[cfg(feature = "memory")]
#[test]
fn memory_store_is_explicitly_injected() {
    use tower_rate_limiter::{MemoryStore, MemoryStoreError};

    assert_eq!(MemoryStoreError::InstantOutOfRange, MemoryStoreError::InstantOutOfRange);

    let policy = FixedWindow::new(MemoryStore::new(), 7, Duration::from_secs(30)).expect("valid fixed-window policy");
    let _layer = RateLimitLayer::builder(StaticKey)
        .with_policy(policy)
        .build()
        .expect("explicit memory store");
}

#[cfg(all(feature = "redis", any(feature = "runtime-tokio", feature = "runtime-smol")))]
#[test]
fn redis_store_errors_are_public_and_equatable() {
    use tower_rate_limiter::RedisStoreError;

    assert_eq!(RedisStoreError::WindowTooShort, RedisStoreError::WindowTooShort);
}

#[test]
fn invalid_window_and_policy_are_rejected_at_build() {
    let too_short = FixedWindow::new(TestStore, 1, Duration::from_micros(999));
    assert!(matches!(too_short, Err(ConfigError::WindowTooShort(_, _))));

    let policy = FixedWindow::new(TestStore, 1, Duration::from_secs(1)).expect("valid fixed-window policy");
    let empty_policy = RateLimitLayer::builder(StaticKey)
        .policy_name("")
        .with_policy(policy)
        .build();
    assert!(matches!(empty_policy, Err(ConfigError::EmptyPolicyName)));

    let policy = FixedWindow::new(TestStore, 1, Duration::from_secs(1)).expect("valid fixed-window policy");
    let invalid_policy = RateLimitLayer::builder(StaticKey)
        .policy_name("not\nstructured")
        .with_policy(policy)
        .build();
    assert!(matches!(invalid_policy, Err(ConfigError::InvalidPolicyName)));
}

#[test]
fn ip_key_extractor_reads_a_tower_socket_addr_extension() {
    let address: SocketAddr = "192.0.2.7:443".parse().unwrap();
    let mut request = Request::new(());
    request.extensions_mut().insert(address);

    assert_eq!(IpKeyExtractor::new().extract(&request).unwrap(), address.ip());
}

#[test]
fn client_ip_key_extractor_prefers_a_client_ip_header_over_the_peer() {
    let peer: SocketAddr = "192.0.2.7:443".parse().unwrap();
    let mut request = Request::new(());
    request.extensions_mut().insert(peer);
    request
        .headers_mut()
        .insert(header::FORWARDED, "for=198.51.100.8".parse().unwrap());

    assert_eq!(
        ClientIpKeyExtractor::new().extract(&request).unwrap(),
        "198.51.100.8".parse::<IpAddr>().unwrap()
    );
}

#[test]
fn client_ip_key_extractor_rejects_an_invalid_client_ip_header() {
    let peer: SocketAddr = "192.0.2.7:443".parse().unwrap();
    let mut request = Request::new(());
    request.extensions_mut().insert(peer);
    request
        .headers_mut()
        .insert(header::FORWARDED, "for=not-an-ip".parse().unwrap());

    let error = ClientIpKeyExtractor::new()
        .extract(&request)
        .expect_err("an invalid client IP header must not fall back to the peer");

    assert!(matches!(
        error,
        RateLimitError::Key(code, _message) if code == "invalid_client_ip"
    ));
}

#[test]
fn client_ip_key_extractor_falls_back_to_the_peer() {
    let peer: SocketAddr = "192.0.2.7:443".parse().unwrap();
    let mut request = Request::new(());
    request.extensions_mut().insert(peer);

    assert_eq!(ClientIpKeyExtractor::new().extract(&request).unwrap(), peer.ip());
}

#[test]
fn client_ip_key_extractor_reports_when_no_address_is_available() {
    let error = ClientIpKeyExtractor::new()
        .extract(&Request::new(()))
        .expect_err("missing client and peer IP must fail");

    assert!(matches!(
        error,
        RateLimitError::Key(code, _message) if code == "client_ip_unavailable"
    ));
}

#[test]
fn ip_key_extractor_ignores_untrusted_client_ip_headers() {
    let peer: SocketAddr = "192.0.2.7:443".parse().unwrap();
    let mut request = Request::new(());
    request.extensions_mut().insert(peer);
    request
        .headers_mut()
        .insert(header::FORWARDED, "for=198.51.100.8".parse().unwrap());

    assert_eq!(IpKeyExtractor::new().extract(&request).unwrap(), peer.ip());
}

#[test]
fn trusted_proxy_client_ip_key_extractor_requires_a_socket_peer() {
    let mut request = Request::new(());
    request
        .headers_mut()
        .insert(header::FORWARDED, "for=198.51.100.8".parse().unwrap());

    let error = TrustedProxyClientIpKeyExtractor::new(|_| true)
        .extract(&request)
        .expect_err("a Header alone must not establish the client identity");

    assert!(matches!(
        error,
        RateLimitError::Key(code, _message) if code == "socket_ip_unavailable"
    ));
}

#[test]
fn trusted_proxy_client_ip_key_extractor_ignores_headers_from_an_untrusted_peer() {
    let peer: SocketAddr = "192.0.2.7:443".parse().unwrap();
    let mut request = Request::new(());
    request.extensions_mut().insert(peer);
    request
        .headers_mut()
        .insert(header::FORWARDED, "for=198.51.100.8".parse().unwrap());

    let extractor = TrustedProxyClientIpKeyExtractor::new(|_| false);

    assert_eq!(extractor.extract(&request).unwrap(), peer.ip());
}

#[test]
fn trusted_proxy_client_ip_key_extractor_does_not_parse_headers_from_an_untrusted_peer() {
    let peer: SocketAddr = "192.0.2.7:443".parse().unwrap();
    let mut request = Request::new(());
    request.extensions_mut().insert(peer);
    request
        .headers_mut()
        .insert(header::FORWARDED, "for=not-an-ip".parse().unwrap());

    let extractor = TrustedProxyClientIpKeyExtractor::new(|_| false);

    assert_eq!(extractor.extract(&request).unwrap(), peer.ip());
}

#[test]
fn trusted_proxy_client_ip_key_extractor_uses_headers_from_a_trusted_peer() {
    let peer: SocketAddr = "192.0.2.7:443".parse().unwrap();
    let mut request = Request::new(());
    request.extensions_mut().insert(peer);
    request
        .headers_mut()
        .insert(header::FORWARDED, "for=198.51.100.8".parse().unwrap());

    let trusted_peer = peer.ip();
    let extractor = TrustedProxyClientIpKeyExtractor::new(move |candidate| candidate == trusted_peer);

    assert_eq!(
        extractor.extract(&request).unwrap(),
        "198.51.100.8".parse::<IpAddr>().unwrap()
    );
}

#[test]
fn trusted_proxy_client_ip_key_extractor_falls_back_to_a_trusted_peer() {
    let peer: SocketAddr = "192.0.2.7:443".parse().unwrap();
    let mut request = Request::new(());
    request.extensions_mut().insert(peer);

    let extractor = TrustedProxyClientIpKeyExtractor::new(|_| true);

    assert_eq!(extractor.extract(&request).unwrap(), peer.ip());
}

#[test]
fn trusted_proxy_client_ip_key_extractor_fails_closed_on_the_first_present_header() {
    let peer: SocketAddr = "192.0.2.7:443".parse().unwrap();
    let mut request = Request::new(());
    request.extensions_mut().insert(peer);
    request
        .headers_mut()
        .insert(header::FORWARDED, "for=not-an-ip".parse().unwrap());
    request
        .headers_mut()
        .insert("x-forwarded-for", "198.51.100.8".parse().unwrap());

    let error = TrustedProxyClientIpKeyExtractor::new(|_| true)
        .extract(&request)
        .expect_err("an invalid first-present Header must not fall through");

    assert!(matches!(
        error,
        RateLimitError::Key(code, _message) if code == "invalid_client_ip"
    ));
}
