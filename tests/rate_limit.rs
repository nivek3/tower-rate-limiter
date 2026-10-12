use std::{
    collections::HashMap,
    convert::Infallible,
    future::{Future, Ready, ready},
    marker::PhantomData,
    pin::pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use http::{Request, Response, StatusCode};
use tower::{Layer, Service};
use tower_rate_limiter::{
    Decision, FixedWindow, FixedWindowStore, FixedWindowUsage, KeyExtractor, PolicyDecision, PolicyFailureMode,
    RateLimitContext, RateLimitError, RateLimitFields, RateLimitLayer, RateLimitPolicy, ResponseFactory,
    ResponseReason,
};

fn block_on<F: Future>(future: F) -> F::Output {
    let mut context = Context::from_waker(Waker::noop());
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
    }
}

#[derive(Clone, Copy)]
struct StaticKey(&'static str);
impl KeyExtractor for StaticKey {
    type Key = String;
    fn extract<B>(&self, _: &Request<B>) -> Result<Self::Key, RateLimitError> {
        Ok(self.0.to_owned())
    }
}
#[derive(Clone, Copy)]
struct FailingKey;
impl KeyExtractor for FailingKey {
    type Key = String;
    fn extract<B>(&self, _: &Request<B>) -> Result<Self::Key, RateLimitError> {
        Err(RateLimitError::Key("test_key_failed".into(), "key".into()))
    }
}

#[derive(Clone, Default)]
struct FakeStore {
    counts: Arc<Mutex<HashMap<String, u64>>>,
    calls: Arc<AtomicUsize>,
    fail: bool,
    zero_usage: bool,
    reset_after: Option<Duration>,
}
impl FixedWindowStore for FakeStore {
    type Future = Ready<Result<FixedWindowUsage, RateLimitError>>;
    fn increment(&self, key: &str, window: Duration) -> Self::Future {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.fail {
            return ready(Err(RateLimitError::Policy("test_policy_failed".into(), "store".into())));
        }
        if self.zero_usage {
            return ready(Ok(FixedWindowUsage {
                used: 0,
                reset_after: window,
            }));
        }
        let mut counts = self.counts.lock().expect("fake store lock");
        let count = counts.entry(key.to_owned()).or_default();
        *count += 1;
        ready(Ok(FixedWindowUsage {
            used: *count,
            reset_after: self.reset_after.unwrap_or(window),
        }))
    }
}

#[derive(Clone, Copy)]
struct FailingPolicy;
impl RateLimitPolicy for FailingPolicy {
    type Future = Ready<Result<Decision, RateLimitError>>;
    fn check(&self, _: String) -> Self::Future {
        ready(Err(RateLimitError::Policy(
            "test_policy_failed".into(),
            "policy".into(),
        )))
    }
}

#[derive(Clone, Default)]
struct OkService {
    calls: Arc<AtomicUsize>,
    bodies: Arc<Mutex<Vec<Vec<u8>>>>,
}
impl Service<Request<Vec<u8>>> for OkService {
    type Response = Response<Vec<u8>>;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<Vec<u8>>) -> Self::Future {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.bodies.lock().expect("body lock").push(request.body().clone());
        ready(Ok(Response::new(b"ok".to_vec())))
    }
}
#[derive(Clone, Default)]
struct ContextCaptureService {
    contexts: Arc<Mutex<Vec<RateLimitContext>>>,
}
impl Service<Request<Vec<u8>>> for ContextCaptureService {
    type Response = Response<Vec<u8>>;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<Vec<u8>>) -> Self::Future {
        if let Some(context) = request.extensions().get::<RateLimitContext>() {
            self.contexts.lock().expect("context lock").push(context.clone());
        }
        ready(Ok(Response::new(Vec::new())))
    }
}

fn request() -> Request<Vec<u8>> {
    Request::new(Vec::new())
}
fn call_service<S>(service: &mut S, request: Request<Vec<u8>>) -> Result<Response<Vec<u8>>, S::Error>
where
    S: Service<Request<Vec<u8>>, Response = Response<Vec<u8>>>,
{
    block_on(async {
        std::future::poll_fn(|cx| service.poll_ready(cx)).await?;
        service.call(request).await
    })
}
fn call_status<S>(service: &mut S) -> StatusCode
where
    S: Service<Request<Vec<u8>>, Response = Response<Vec<u8>>>,
    S::Error: std::fmt::Debug,
{
    call_service(service, request()).expect("response").status()
}
fn assert_header<B>(response: &Response<B>, name: &str, value: &str) {
    assert_eq!(response.headers().get(name).expect("header"), value);
}
fn layer(store: FakeStore, limit: u64) -> RateLimitLayer<StaticKey, FixedWindow<FakeStore>> {
    RateLimitLayer::builder(StaticKey("caller"))
        .with_policy(FixedWindow::new(store, limit, Duration::from_secs(60)).expect("valid policy"))
        .build()
        .expect("valid layer")
}

#[test]
fn fixed_window_first_limit_requests_pass_and_next_request_is_rate_limited() {
    let store = FakeStore::default();
    let calls = store.calls.clone();
    let mut service = layer(store, 2).layer(OkService::default());
    assert_eq!(call_status(&mut service), StatusCode::OK);
    assert_eq!(call_status(&mut service), StatusCode::OK);
    assert_eq!(call_status(&mut service), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(calls.load(Ordering::Relaxed), 3);
}

#[derive(Clone, Copy)]
struct TrustedCaller;
#[test]
fn skip_predicate_bypasses_key_policy_and_inner_metadata() {
    let inner = OkService::default();
    let calls = inner.calls.clone();
    let bodies = inner.bodies.clone();
    let mut service = RateLimitLayer::builder(FailingKey)
        .with_policy(FailingPolicy)
        .skip(|r| r.extensions().get::<TrustedCaller>().is_some())
        .build()
        .expect("layer")
        .layer(inner);
    let mut bypassed = request();
    bypassed.extensions_mut().insert(TrustedCaller);
    *bypassed.body_mut() = b"payload".to_vec();
    let response = call_service(&mut service, bypassed).expect("bypassed");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(bodies.lock().expect("body lock").as_slice(), [b"payload".to_vec()]);
    assert!(response.headers().get("ratelimit").is_none());
    assert!(response.extensions().get::<RateLimitContext>().is_none());
    assert_eq!(call_status(&mut service), StatusCode::INTERNAL_SERVER_ERROR);
}

#[test]
fn policy_failure_is_rejected_or_allowed_as_configured() {
    let inner = OkService::default();
    let calls = inner.calls.clone();
    let mut reject = RateLimitLayer::builder(StaticKey("caller"))
        .with_policy(FailingPolicy)
        .build()
        .expect("layer")
        .layer(inner);
    assert_eq!(call_status(&mut reject), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    let inner = OkService::default();
    let calls = inner.calls.clone();
    let mut allow = RateLimitLayer::builder(StaticKey("caller"))
        .policy_failure_mode(PolicyFailureMode::Allow)
        .with_policy(FailingPolicy)
        .build()
        .expect("layer")
        .layer(inner);
    assert_eq!(call_status(&mut allow), StatusCode::OK);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[test]
fn invalid_fixed_window_usage_is_a_policy_failure() {
    let mut service = layer(
        FakeStore {
            zero_usage: true,
            ..FakeStore::default()
        },
        1,
    )
    .layer(OkService::default());
    assert_eq!(call_status(&mut service), StatusCode::SERVICE_UNAVAILABLE);
}

#[derive(Debug)]
struct TestError(&'static str);
impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for TestError {}
#[derive(Clone, Copy)]
struct ErrorService;
impl Service<Request<Vec<u8>>> for ErrorService {
    type Response = Response<Vec<u8>>;
    type Error = TestError;
    type Future = Ready<Result<Self::Response, Self::Error>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: Request<Vec<u8>>) -> Self::Future {
        ready(Err(TestError("inner")))
    }
}
#[test]
fn inner_error_type_is_preserved() {
    assert_eq!(
        call_service(&mut layer(FakeStore::default(), 1).layer(ErrorService), request())
            .expect_err("inner")
            .0,
        "inner"
    );
}

#[derive(Clone, Copy)]
struct ReasonFactory;
impl ResponseFactory<Vec<u8>, Vec<u8>> for ReasonFactory {
    fn build(&self, _: Request<Vec<u8>>, reason: ResponseReason) -> Response<Vec<u8>> {
        let status = match reason {
            ResponseReason::RateLimited(policy_decision) => {
                assert_policy_decision(&policy_decision, "default-policy", 0, 0, Duration::from_secs(60));
                StatusCode::IM_A_TEAPOT
            },
            ResponseReason::Error(RateLimitError::Key(_, _)) => StatusCode::BAD_REQUEST,
            ResponseReason::Error(RateLimitError::Policy(_, _)) => StatusCode::NOT_ACCEPTABLE,
        };
        Response::builder().status(status).body(Vec::new()).expect("response")
    }
}
fn assert_policy_decision(policy_decision: &PolicyDecision, name: &str, limit: u64, remaining: u64, window: Duration) {
    assert_eq!(policy_decision.name(), name);
    assert_eq!(policy_decision.decision().limit(), limit);
    assert_eq!(policy_decision.decision().remaining(), remaining);
    assert_eq!(policy_decision.decision().window(), window);
}
#[test]
fn custom_response_factory_receives_policy_decisions_and_errors() {
    let mut limited = RateLimitLayer::builder(StaticKey("caller"))
        .with_policy(FixedWindow::new(FakeStore::default(), 0, Duration::from_secs(60)).expect("policy"))
        .response_factory(ReasonFactory)
        .build()
        .expect("layer")
        .layer(OkService::default());
    assert_eq!(call_status(&mut limited), StatusCode::IM_A_TEAPOT);
    let mut failed = RateLimitLayer::builder(StaticKey("caller"))
        .with_policy(FailingPolicy)
        .response_factory(ReasonFactory)
        .build()
        .expect("layer")
        .layer(OkService::default());
    assert_eq!(call_status(&mut failed), StatusCode::NOT_ACCEPTABLE);
}

#[derive(Clone)]
struct Ordered(Arc<Mutex<Vec<&'static str>>>);
impl Ordered {
    fn record(&self, value: &'static str) {
        self.0.lock().expect("order").push(value);
    }
}
impl KeyExtractor for Ordered {
    type Key = String;
    fn extract<B>(&self, _: &Request<B>) -> Result<String, RateLimitError> {
        self.record("key");
        Ok("caller".into())
    }
}
impl RateLimitPolicy for Ordered {
    type Future = Ready<Result<Decision, RateLimitError>>;
    fn check(&self, _: String) -> Self::Future {
        self.record("policy");
        ready(Ok(Decision::allowed(
            1,
            Duration::from_secs(60),
            0,
            Duration::from_secs(60),
        )))
    }
}
#[test]
fn request_flow_resolves_key_then_policy() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let ordered = Ordered(events.clone());
    let mut service = RateLimitLayer::builder(ordered.clone())
        .with_policy(ordered)
        .build()
        .expect("layer")
        .layer(OkService::default());
    call_service(&mut service, request()).expect("allowed");
    assert_eq!(*events.lock().expect("order"), vec!["key", "policy"]);
}

#[test]
fn policy_receives_escaped_scoped_and_encoded_key() {
    #[derive(Clone)]
    struct CapturingPolicy(Arc<Mutex<Vec<String>>>);
    impl RateLimitPolicy for CapturingPolicy {
        type Future = Ready<Result<Decision, RateLimitError>>;
        fn check(&self, key: String) -> Self::Future {
            self.0.lock().expect("keys").push(key);
            ready(Ok(Decision::allowed(
                1,
                Duration::from_secs(60),
                0,
                Duration::from_secs(60),
            )))
        }
    }
    let keys = Arc::new(Mutex::new(Vec::new()));
    let mut service = RateLimitLayer::builder(StaticKey("c%d:e"))
        .policy_name("a:b")
        .with_key_encoder(|key| format!("encoded:{key}"))
        .with_policy(CapturingPolicy(keys.clone()))
        .build()
        .expect("layer")
        .layer(OkService::default());
    call_service(&mut service, request()).expect("allowed");
    assert_eq!(keys.lock().expect("keys").as_slice(), ["encoded:a%3Ab:c%25d%3Ae"]);
}

#[test]
fn policy_names_isolate_fixed_window_usage_when_one_store_is_reused() {
    let store = FakeStore::default();
    let policy = |limit| FixedWindow::new(store.clone(), limit, Duration::from_secs(60)).expect("policy");
    let mut first = RateLimitLayer::builder(StaticKey("caller"))
        .policy_name("first")
        .with_policy(policy(1))
        .build()
        .expect("layer")
        .layer(OkService::default());
    let mut second = RateLimitLayer::builder(StaticKey("caller"))
        .policy_name("second")
        .with_policy(policy(1))
        .build()
        .expect("layer")
        .layer(OkService::default());
    assert_eq!(call_status(&mut first), StatusCode::OK);
    assert_eq!(call_status(&mut first), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(call_status(&mut second), StatusCode::OK);
}

struct DelayedFuture {
    polled: bool,
}
impl Future for DelayedFuture {
    type Output = Result<Decision, RateLimitError>;
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.polled {
            Poll::Ready(Ok(Decision::allowed(
                1,
                Duration::from_secs(60),
                0,
                Duration::from_secs(60),
            )))
        } else {
            self.polled = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}
#[derive(Clone, Copy)]
struct DelayedPolicy;
impl RateLimitPolicy for DelayedPolicy {
    type Future = DelayedFuture;
    fn check(&self, _: String) -> Self::Future {
        DelayedFuture { polled: false }
    }
}
#[test]
fn asynchronous_policy_is_awaited() {
    let mut service = RateLimitLayer::builder(StaticKey("caller"))
        .with_policy(DelayedPolicy)
        .build()
        .expect("layer")
        .layer(OkService::default());
    assert_eq!(call_status(&mut service), StatusCode::OK);
}

#[test]
fn allowed_response_has_draft_eleven_fields_and_context() {
    let inner = ContextCaptureService::default();
    let contexts = inner.contexts.clone();
    let mut service = layer(FakeStore::default(), 2).layer(inner);
    let response = call_service(&mut service, request()).expect("allowed");
    assert_header(&response, "RateLimit-Policy", "\"default-policy\";q=2;w=60");
    assert_header(&response, "RateLimit", "\"default-policy\";r=1;t=60");
    let context = contexts.lock().expect("contexts").first().cloned().expect("context");
    assert_eq!(context.decisions().len(), 1);
    assert_policy_decision(&context.decisions()[0], "default-policy", 2, 1, Duration::from_secs(60));
}

#[derive(Clone, Copy)]
struct DifferentRetryPolicy;
impl RateLimitPolicy for DifferentRetryPolicy {
    type Future = Ready<Result<Decision, RateLimitError>>;
    fn check(&self, _: String) -> Self::Future {
        ready(Ok(Decision::rate_limited(
            10,
            Duration::from_secs(60),
            Duration::from_secs(45),
            Duration::from_secs(2),
        )))
    }
}
#[test]
fn rejected_response_uses_retry_after_for_retry_header_and_reset_after_for_rate_limit() {
    let mut service = RateLimitLayer::builder(StaticKey("caller"))
        .with_policy(DifferentRetryPolicy)
        .build()
        .expect("layer")
        .layer(OkService::default());
    let response = call_service(&mut service, request()).expect("blocked");
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_header(&response, "RateLimit", "\"default-policy\";r=0;t=45");
    assert_header(&response, "Retry-After", "2");
}

#[test]
fn nested_layers_append_policy_fields_and_context_entries() {
    let first = RateLimitLayer::builder(StaticKey("caller"))
        .policy_name("first")
        .with_policy(FixedWindow::new(FakeStore::default(), 2, Duration::from_secs(60)).expect("policy"))
        .build()
        .expect("layer");
    let second = RateLimitLayer::builder(StaticKey("caller"))
        .policy_name("second")
        .with_policy(FixedWindow::new(FakeStore::default(), 3, Duration::from_secs(60)).expect("policy"))
        .build()
        .expect("layer");
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let mut service = second.layer(first.layer(ContextCaptureService {
        contexts: contexts.clone(),
    }));
    let response = call_service(&mut service, request()).expect("allowed");
    assert_eq!(
        response
            .headers()
            .get_all("RateLimit-Policy")
            .iter()
            .map(|v| v.to_str().expect("header"))
            .collect::<Vec<_>>(),
        vec!["\"first\";q=2;w=60", "\"second\";q=3;w=60"]
    );
    assert_eq!(
        contexts.lock().expect("contexts")[0]
            .decisions()
            .iter()
            .map(|state| state.name())
            .collect::<Vec<_>>(),
        vec!["second", "first"]
    );
}

#[test]
fn field_revisions_and_rounding_are_preserved() {
    assert_eq!(RateLimitFields::default(), RateLimitFields::Draft11);
    let mut legacy = RateLimitLayer::builder(StaticKey("caller"))
        .rate_limit_fields(RateLimitFields::Draft7)
        .with_policy(FixedWindow::new(FakeStore::default(), 1, Duration::from_secs(1)).expect("policy"))
        .build()
        .expect("layer")
        .layer(OkService::default());
    assert_header(
        &call_service(&mut legacy, request()).expect("allowed"),
        "RateLimit",
        "limit=1, remaining=0, reset=1",
    );
    let mut disabled = RateLimitLayer::builder(StaticKey("caller"))
        .rate_limit_fields(RateLimitFields::Disabled)
        .with_policy(FixedWindow::new(FakeStore::default(), 0, Duration::from_millis(1)).expect("policy"))
        .build()
        .expect("layer")
        .layer(OkService::default());
    let response = call_service(&mut disabled, request()).expect("blocked");
    assert!(response.headers().get("RateLimit").is_none());
    assert_header(&response, "Retry-After", "1");
}

#[derive(Debug)]
struct ReadinessService {
    id: usize,
    next_id: Arc<AtomicUsize>,
    events: ReadinessEvents,
}

type ReadinessEvents = Arc<Mutex<Vec<(usize, &'static str)>>>;

impl ReadinessService {
    fn new() -> (Self, ReadinessEvents) {
        let events = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                id: 0,
                next_id: Arc::new(AtomicUsize::new(1)),
                events: events.clone(),
            },
            events,
        )
    }
}
impl Clone for ReadinessService {
    fn clone(&self) -> Self {
        Self {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            next_id: self.next_id.clone(),
            events: self.events.clone(),
        }
    }
}
impl Service<Request<Vec<u8>>> for ReadinessService {
    type Response = Response<Vec<u8>>;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.events.lock().expect("events").push((self.id, "ready"));
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: Request<Vec<u8>>) -> Self::Future {
        self.events.lock().expect("events").push((self.id, "call"));
        ready(Ok(Response::new(Vec::new())))
    }
}
#[test]
fn allowed_request_uses_the_inner_instance_reserved_by_poll_ready() {
    let (inner, events) = ReadinessService::new();
    let mut service = layer(FakeStore::default(), 1).layer(inner);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(service.poll_ready(&mut cx), Poll::Ready(Ok(()))));
    assert_eq!(
        block_on(service.call(request())).expect("response").status(),
        StatusCode::OK
    );
    assert_eq!(*events.lock().expect("events"), vec![(0, "ready"), (0, "call")]);
}

#[derive(Clone)]
struct DropTrackingService {
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
impl Drop for DropTrackingService {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}
impl Service<Request<Vec<u8>>> for DropTrackingService {
    type Response = Response<Vec<u8>>;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: Request<Vec<u8>>) -> Self::Future {
        self.calls.fetch_add(1, Ordering::Relaxed);
        ready(Ok(Response::new(Vec::new())))
    }
}
struct PendingDropFuture<T> {
    drops: Arc<AtomicUsize>,
    _output: PhantomData<fn() -> T>,
}
impl<T> Future for PendingDropFuture<T> {
    type Output = T;
    fn poll(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<T> {
        Poll::Pending
    }
}
impl<T> Drop for PendingDropFuture<T> {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}
#[derive(Clone)]
struct PendingPolicy {
    drops: Arc<AtomicUsize>,
}
impl RateLimitPolicy for PendingPolicy {
    type Future = PendingDropFuture<Result<Decision, RateLimitError>>;
    fn check(&self, _: String) -> Self::Future {
        PendingDropFuture {
            drops: self.drops.clone(),
            _output: PhantomData,
        }
    }
}
#[test]
fn cancelling_a_pending_policy_releases_future_and_reserved_inner_once() {
    let calls = Arc::new(AtomicUsize::new(0));
    let inner_drops = Arc::new(AtomicUsize::new(0));
    let policy_drops = Arc::new(AtomicUsize::new(0));
    let inner = DropTrackingService {
        calls: calls.clone(),
        drops: inner_drops.clone(),
    };
    let mut service = RateLimitLayer::builder(StaticKey("caller"))
        .with_policy(PendingPolicy {
            drops: policy_drops.clone(),
        })
        .build()
        .expect("layer")
        .layer(inner);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(service.poll_ready(&mut cx), Poll::Ready(Ok(()))));
    let mut response = Box::pin(service.call(request()));
    assert!(matches!(response.as_mut().poll(&mut cx), Poll::Pending));
    drop(response);
    assert_eq!(policy_drops.load(Ordering::Relaxed), 1);
    assert_eq!(inner_drops.load(Ordering::Relaxed), 1);
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[test]
fn empty_client_keys_are_passed_to_the_fixed_window_store() {
    let store = FakeStore::default();
    let stored_keys = Arc::clone(&store.counts);
    let policy = FixedWindow::new(store, 1, Duration::from_secs(60)).expect("valid policy");
    let mut service = RateLimitLayer::builder(StaticKey(""))
        .with_policy(policy)
        .build()
        .expect("valid layer")
        .layer(OkService::default());

    assert_eq!(call_status(&mut service), StatusCode::OK);
    assert!(stored_keys.lock().expect("store lock").contains_key("default-policy:"));
}

#[test]
fn fixed_window_store_error_is_rejected_or_allowed_as_configured() {
    let rejecting = FakeStore {
        fail: true,
        ..FakeStore::default()
    };
    let inner = OkService::default();
    let inner_calls = Arc::clone(&inner.calls);
    let mut rejecting = layer(rejecting, 1).layer(inner);

    assert_eq!(call_status(&mut rejecting), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(inner_calls.load(Ordering::Relaxed), 0);

    let allowing = FakeStore {
        fail: true,
        ..FakeStore::default()
    };
    let policy = FixedWindow::new(allowing, 1, Duration::from_secs(60)).expect("valid policy");
    let inner = OkService::default();
    let inner_calls = Arc::clone(&inner.calls);
    let mut allowing = RateLimitLayer::builder(StaticKey("caller"))
        .policy_failure_mode(PolicyFailureMode::Allow)
        .with_policy(policy)
        .build()
        .expect("valid layer")
        .layer(inner);

    assert_eq!(call_status(&mut allowing), StatusCode::OK);
    assert_eq!(inner_calls.load(Ordering::Relaxed), 1);
}

#[test]
fn allow_passes_through_when_fixed_window_usage_is_invalid() {
    let policy = FixedWindow::new(
        FakeStore {
            zero_usage: true,
            ..FakeStore::default()
        },
        1,
        Duration::from_secs(60),
    )
    .expect("valid policy");
    let inner = OkService::default();
    let inner_calls = Arc::clone(&inner.calls);
    let mut service = RateLimitLayer::builder(StaticKey("caller"))
        .policy_failure_mode(PolicyFailureMode::Allow)
        .with_policy(policy)
        .build()
        .expect("valid layer")
        .layer(inner);

    assert_eq!(call_status(&mut service), StatusCode::OK);
    assert_eq!(inner_calls.load(Ordering::Relaxed), 1);
}

#[test]
fn custom_response_factory_decides_how_to_handle_key_errors() {
    let mut service = RateLimitLayer::builder(FailingKey)
        .with_policy(FailingPolicy)
        .response_factory(ReasonFactory)
        .build()
        .expect("valid layer")
        .layer(OkService::default());

    assert_eq!(call_status(&mut service), StatusCode::BAD_REQUEST);
}

#[test]
fn scoped_key_is_raw_without_a_key_encoder() {
    let store = FakeStore::default();
    let stored_keys = Arc::clone(&store.counts);
    let policy = FixedWindow::new(store, 1, Duration::from_secs(60)).expect("valid policy");
    let mut service = RateLimitLayer::builder(StaticKey("caller"))
        .policy_name("api")
        .with_policy(policy)
        .build()
        .expect("valid layer")
        .layer(OkService::default());

    call_service(&mut service, request()).expect("allowed response");
    assert!(stored_keys.lock().expect("store lock").contains_key("api:caller"));
}

#[test]
fn scoped_key_escapes_colons_and_percents_before_fixed_window_store() {
    let store = FakeStore::default();
    let stored_keys = Arc::clone(&store.counts);
    let policy = FixedWindow::new(store, 1, Duration::from_secs(60)).expect("valid policy");
    let mut service = RateLimitLayer::builder(StaticKey("c%d:e"))
        .policy_name("a:b")
        .with_policy(policy)
        .build()
        .expect("valid layer")
        .layer(OkService::default());

    call_service(&mut service, request()).expect("allowed response");
    assert!(stored_keys.lock().expect("store lock").contains_key("a%3Ab:c%25d%3Ae"));
}

#[test]
fn same_policy_name_shares_fixed_window_usage_when_windows_differ() {
    let store = FakeStore::default();
    let short_policy = FixedWindow::new(store.clone(), 1, Duration::from_secs(30)).expect("valid policy");
    let long_policy = FixedWindow::new(store, 1, Duration::from_secs(60)).expect("valid policy");
    let mut short = RateLimitLayer::builder(StaticKey("caller"))
        .policy_name("same-policy")
        .with_policy(short_policy)
        .build()
        .expect("valid layer")
        .layer(OkService::default());
    let mut long = RateLimitLayer::builder(StaticKey("caller"))
        .policy_name("same-policy")
        .with_policy(long_policy)
        .build()
        .expect("valid layer")
        .layer(OkService::default());

    assert_eq!(call_status(&mut short), StatusCode::OK);
    assert_eq!(call_status(&mut long), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(call_status(&mut short), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(call_status(&mut long), StatusCode::TOO_MANY_REQUESTS);
}

#[test]
fn blocked_fixed_window_response_has_rate_fields_and_retry_after() {
    let mut service = layer(FakeStore::default(), 1).layer(OkService::default());
    let _ = call_service(&mut service, request()).expect("allowed response");
    let response = call_service(&mut service, request()).expect("blocked response");

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_header(&response, "RateLimit-Policy", "\"default-policy\";q=1;w=60");
    assert_header(&response, "RateLimit", "\"default-policy\";r=0;t=60");
    assert_header(&response, "Retry-After", "60");
}

#[test]
fn draft11_policy_names_escape_structured_string_delimiters() {
    let policy = FixedWindow::new(FakeStore::default(), 1, Duration::from_secs(60)).expect("valid policy");
    let mut service = RateLimitLayer::builder(StaticKey("caller"))
        .policy_name("a\"\\b")
        .with_policy(policy)
        .build()
        .expect("valid layer")
        .layer(OkService::default());

    let response = call_service(&mut service, request()).expect("allowed response");
    assert_header(&response, "RateLimit-Policy", "\"a\\\"\\\\b\";q=1;w=60");
    assert_header(&response, "RateLimit", "\"a\\\"\\\\b\";r=0;t=60");
}

#[test]
fn draft7_fields_include_policy_and_rate_limit_dictionaries() {
    let policy = FixedWindow::new(FakeStore::default(), 1, Duration::from_secs(60)).expect("valid policy");
    let mut service = RateLimitLayer::builder(StaticKey("caller"))
        .rate_limit_fields(RateLimitFields::Draft7)
        .with_policy(policy)
        .build()
        .expect("valid layer")
        .layer(OkService::default());

    let response = call_service(&mut service, request()).expect("allowed response");
    assert_header(&response, "RateLimit-Policy", "1;w=60");
    assert_header(&response, "RateLimit", "limit=1, remaining=0, reset=60");
}

#[test]
fn zero_reset_duration_rounds_up_to_one_second() {
    let store = FakeStore {
        reset_after: Some(Duration::ZERO),
        ..FakeStore::default()
    };
    let mut service = layer(store, 1).layer(OkService::default());
    let _ = call_service(&mut service, request()).expect("allowed response");
    let response = call_service(&mut service, request()).expect("blocked response");

    assert_header(&response, "RateLimit", "\"default-policy\";r=0;t=1");
    assert_header(&response, "Retry-After", "1");
}

#[test]
fn active_subsecond_fixed_window_durations_round_up_to_one_second() {
    let policy = FixedWindow::new(FakeStore::default(), 1, Duration::from_millis(1)).expect("valid policy");
    let mut service = RateLimitLayer::builder(StaticKey("caller"))
        .with_policy(policy)
        .build()
        .expect("valid layer")
        .layer(OkService::default());
    let _ = call_service(&mut service, request()).expect("allowed response");
    let response = call_service(&mut service, request()).expect("blocked response");

    assert_header(&response, "RateLimit-Policy", "\"default-policy\";q=1;w=1");
    assert_header(&response, "RateLimit", "\"default-policy\";r=0;t=1");
    assert_header(&response, "Retry-After", "1");
}

#[test]
fn unavailable_responses_do_not_claim_quota_metadata() {
    let store = FakeStore {
        fail: true,
        ..FakeStore::default()
    };
    let mut service = layer(store, 1).layer(OkService::default());
    let response = call_service(&mut service, request()).expect("policy error response");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(response.headers().get("RateLimit").is_none());
    assert!(response.headers().get("RateLimit-Policy").is_none());
    assert!(response.headers().get("Retry-After").is_none());
}

#[test]
fn rejected_request_drops_the_reserved_inner_without_calling_it() {
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let inner = DropTrackingService {
        calls: Arc::clone(&calls),
        drops: Arc::clone(&drops),
    };
    let policy = FixedWindow::new(FakeStore::default(), 0, Duration::from_secs(60)).expect("valid policy");
    let mut service = RateLimitLayer::builder(StaticKey("caller"))
        .with_policy(policy)
        .build()
        .expect("valid layer")
        .layer(inner);

    assert_eq!(call_status(&mut service), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[derive(Clone)]
struct PendingStore {
    future_drops: Arc<AtomicUsize>,
}

impl FixedWindowStore for PendingStore {
    type Future = PendingDropFuture<Result<FixedWindowUsage, RateLimitError>>;

    fn increment(&self, _key: &str, _window: Duration) -> Self::Future {
        PendingDropFuture {
            drops: Arc::clone(&self.future_drops),
            _output: PhantomData,
        }
    }
}

#[test]
fn cancelling_a_pending_fixed_window_store_releases_future_and_reserved_inner_once() {
    let calls = Arc::new(AtomicUsize::new(0));
    let inner_drops = Arc::new(AtomicUsize::new(0));
    let store_drops = Arc::new(AtomicUsize::new(0));
    let inner = DropTrackingService {
        calls: Arc::clone(&calls),
        drops: Arc::clone(&inner_drops),
    };
    let policy = FixedWindow::new(
        PendingStore {
            future_drops: Arc::clone(&store_drops),
        },
        1,
        Duration::from_secs(60),
    )
    .expect("valid policy");
    let mut service = RateLimitLayer::builder(StaticKey("caller"))
        .with_policy(policy)
        .build()
        .expect("valid layer")
        .layer(inner);
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(service.poll_ready(&mut context), Poll::Ready(Ok(()))));
    let mut response = Box::pin(service.call(request()));
    assert!(matches!(response.as_mut().poll(&mut context), Poll::Pending));
    drop(response);

    assert_eq!(store_drops.load(Ordering::Relaxed), 1);
    assert_eq!(inner_drops.load(Ordering::Relaxed), 1);
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}
