#![cfg(feature = "tracing")]

use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    fmt,
    future::{Ready, ready},
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use http::{Request, Response, StatusCode};
use tower::{Layer, Service, ServiceExt};
use tower_rate_limiter::{
    Decision, FixedWindow, FixedWindowStore, FixedWindowUsage, KeyExtractor, PolicyFailureMode, RateLimitError,
    RateLimitLayer, RateLimitPolicy,
};
use tracing::{
    Event, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};

#[derive(Clone, Copy)]
struct StaticKey;

impl KeyExtractor for StaticKey {
    type Key = &'static str;
    fn extract<B>(&self, _request: &Request<B>) -> Result<Self::Key, RateLimitError> {
        Ok("caller")
    }
}

#[derive(Clone, Copy)]
struct FailingPolicy;

impl RateLimitPolicy for FailingPolicy {
    type Future = Ready<Result<Decision, RateLimitError>>;
    fn check(&self, _key: String) -> Self::Future {
        ready(Err(RateLimitError::Policy(
            String::from("test_policy_failed"),
            String::from("redis://user:secret@example.invalid"),
        )))
    }
}

#[derive(Clone, Copy)]
struct FailingKey;

impl KeyExtractor for FailingKey {
    type Key = &'static str;
    fn extract<B>(&self, _request: &Request<B>) -> Result<Self::Key, RateLimitError> {
        Err(RateLimitError::Key(
            String::from("test_key_failed"),
            String::from("key unavailable"),
        ))
    }
}

#[derive(Clone, Copy)]
struct InvalidUsageStore;

impl FixedWindowStore for InvalidUsageStore {
    type Future = Ready<Result<FixedWindowUsage, RateLimitError>>;

    fn increment(&self, _key: &str, window: Duration) -> Self::Future {
        ready(Ok(FixedWindowUsage {
            used: 0,
            reset_after: window,
        }))
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

fn call_with_mode(mode: PolicyFailureMode) -> StatusCode {
    let service = RateLimitLayer::builder(StaticKey)
        .policy_name("login")
        .policy_failure_mode(mode)
        .with_policy(FailingPolicy)
        .build()
        .expect("valid layer")
        .layer(OkService);
    smol::block_on(service.oneshot(Request::new(())))
        .expect("infallible service")
        .status()
}

fn call_with_level(level: tracing::Level) -> StatusCode {
    let service = RateLimitLayer::builder(StaticKey)
        .policy_name("login")
        .policy_failure_mode(PolicyFailureMode::Allow)
        .policy_failure_tracing_level(level)
        .with_policy(FailingPolicy)
        .build()
        .expect("valid layer")
        .layer(OkService);
    smol::block_on(service.oneshot(Request::new(())))
        .expect("infallible service")
        .status()
}

#[derive(Clone, Debug)]
struct CapturedEvent {
    level: tracing::Level,
    target: &'static str,
    fields: HashMap<String, String>,
}

#[derive(Clone, Default)]
struct EventSubscriber {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl Subscriber for EventSubscriber {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _attributes: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _span: &Id, _values: &Record<'_>) {}
    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}
    fn enter(&self, _span: &Id) {}
    fn exit(&self, _span: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        self.events.lock().expect("event lock").push(CapturedEvent {
            level: *event.metadata().level(),
            target: event.metadata().target(),
            fields: visitor.fields,
        });
    }
}

#[derive(Default)]
struct FieldVisitor {
    fields: HashMap<String, String>,
}

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields.insert(field.name().to_owned(), value.to_owned());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.fields.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

#[test]
fn policy_failure_level_is_configurable_per_policy() {
    let subscriber = EventSubscriber::default();
    let events = Arc::clone(&subscriber.events);
    let levels = [
        tracing::Level::ERROR,
        tracing::Level::WARN,
        tracing::Level::INFO,
        tracing::Level::DEBUG,
        tracing::Level::TRACE,
    ];
    tracing::subscriber::with_default(subscriber, || {
        for level in levels {
            assert_eq!(call_with_level(level), StatusCode::OK);
        }
    });
    assert_eq!(
        events
            .lock()
            .expect("event lock")
            .iter()
            .map(|event| event.level)
            .collect::<Vec<_>>(),
        levels
    );
}

#[test]
fn policy_failures_emit_structured_warnings_without_diagnostic_details() {
    let subscriber = EventSubscriber::default();
    let events = Arc::clone(&subscriber.events);
    tracing::subscriber::with_default(subscriber, || {
        assert_eq!(call_with_mode(PolicyFailureMode::Allow), StatusCode::OK);
        assert_eq!(
            call_with_mode(PolicyFailureMode::Reject),
            StatusCode::SERVICE_UNAVAILABLE
        );
    });
    let events = events.lock().expect("event lock");
    assert_eq!(events.len(), 2);
    for (event, expected_mode) in events.iter().zip(["allow", "reject"]) {
        assert_eq!(event.level, tracing::Level::WARN);
        assert_eq!(event.target, "tower_rate_limiter::policy");
        assert_eq!(
            event.fields.keys().map(String::as_str).collect::<HashSet<_>>(),
            HashSet::from(["message", "event", "policy_name", "failure_mode", "error_code"])
        );
        assert_eq!(event.fields.get("event").map(String::as_str), Some("policy_failure"));
        assert_eq!(event.fields.get("policy_name").map(String::as_str), Some("login"));
        assert_eq!(
            event.fields.get("failure_mode").map(String::as_str),
            Some(expected_mode)
        );
        assert_eq!(
            event.fields.get("error_code").map(String::as_str),
            Some("test_policy_failed")
        );
        assert!(
            event
                .fields
                .values()
                .all(|value| !value.contains("redis://") && !value.contains("secret"))
        );
    }
}

#[test]
fn invalid_fixed_window_usage_emits_policy_failure() {
    let subscriber = EventSubscriber::default();
    let events = Arc::clone(&subscriber.events);
    let policy = FixedWindow::new(InvalidUsageStore, 1, Duration::from_secs(60)).expect("policy");
    let service = RateLimitLayer::builder(StaticKey)
        .policy_name("login")
        .with_policy(policy)
        .build()
        .expect("layer")
        .layer(OkService);

    tracing::subscriber::with_default(subscriber, || {
        assert_eq!(
            smol::block_on(service.oneshot(Request::new(())))
                .expect("response")
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    });

    let events = events.lock().expect("event lock");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].target, "tower_rate_limiter::policy");
    assert_eq!(
        events[0].fields.get("event").map(String::as_str),
        Some("policy_failure")
    );
    assert_eq!(
        events[0].fields.get("error_code").map(String::as_str),
        Some("invalid_usage")
    );
}

#[test]
fn key_failures_do_not_emit_policy_failure() {
    let subscriber = EventSubscriber::default();
    let events = Arc::clone(&subscriber.events);
    let service = RateLimitLayer::builder(FailingKey)
        .with_policy(FailingPolicy)
        .build()
        .expect("valid layer")
        .layer(OkService);
    tracing::subscriber::with_default(subscriber, || {
        assert_eq!(
            smol::block_on(service.oneshot(Request::new(())))
                .expect("response")
                .status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    });
    assert!(events.lock().expect("event lock").is_empty());
}
