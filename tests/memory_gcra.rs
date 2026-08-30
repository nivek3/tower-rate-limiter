use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
    thread,
    time::Duration,
};

use http::{Request, Response, StatusCode};
use tower::{Layer, ServiceExt, service_fn};
use tower_rate_limiter::{
    Decision, GcraQuota, KeyExtractor, MemoryGcra, RateLimitError, RateLimitLayer, RateLimitPolicy,
};

fn quota(rate: u32, burst: u32) -> GcraQuota {
    GcraQuota::new(rate, Duration::from_secs(1), burst).expect("valid GCRA quota")
}

fn check(policy: &MemoryGcra, key: &str) -> Decision {
    let mut future = Box::pin(policy.check(key.into()));
    let mut context = Context::from_waker(Waker::noop());

    match Pin::as_mut(&mut future).poll(&mut context) {
        Poll::Ready(Ok(decision)) => decision,
        Poll::Ready(Err(error)) => panic!("memory GCRA must produce a decision: {error}"),
        Poll::Pending => panic!("memory GCRA checks must complete synchronously"),
    }
}

#[derive(Clone, Copy)]
struct StaticKey;

impl KeyExtractor for StaticKey {
    type Key = &'static str;

    fn extract<B>(&self, _request: &Request<B>) -> Result<Self::Key, RateLimitError> {
        Ok("client")
    }
}

#[test]
fn fresh_key_allows_the_full_burst_and_reports_governor_remaining_capacity() {
    let quota = quota(50, 3);
    let policy = MemoryGcra::new(quota);
    let mut previous_remaining = u64::from(quota.burst());

    for _ in 0..quota.burst() {
        let decision = check(&policy, "client");
        assert!(!decision.is_rate_limited());
        assert_eq!(decision.limit(), u64::from(quota.burst()));
        assert_eq!(decision.window(), quota.window());
        assert!(decision.remaining() < u64::from(quota.burst()));
        assert!(decision.remaining() <= previous_remaining);
        assert!(decision.reset_after() <= quota.window());
        previous_remaining = decision.remaining();
    }

    assert_eq!(previous_remaining, 0);
    assert!(check(&policy, "client").is_rate_limited());
}

#[test]
fn rejected_checks_do_not_add_penalty_and_the_key_recovers_after_retry() {
    let quota = quota(50, 2);
    let policy = MemoryGcra::new(quota);

    assert!(!check(&policy, "client").is_rate_limited());
    assert!(!check(&policy, "client").is_rate_limited());

    let first_rejection = check(&policy, "client");
    let first_retry = first_rejection.retry_after().expect("rejection supplies retry-after");
    assert!(first_rejection.is_rate_limited());
    assert_eq!(first_rejection.remaining(), 0);
    assert!(first_retry > Duration::ZERO);
    assert!(first_rejection.reset_after() >= first_retry);

    let second_rejection = check(&policy, "client");
    let second_retry = second_rejection.retry_after().expect("rejection supplies retry-after");
    assert!(second_rejection.is_rate_limited());
    assert!(
        second_retry <= first_retry,
        "a rejected check must not advance the GCRA penalty: first={first_retry:?}, second={second_retry:?}"
    );

    thread::sleep(second_retry + quota.emission_interval() + Duration::from_millis(5));
    assert!(!check(&policy, "client").is_rate_limited());
}

#[test]
fn client_keys_are_isolated() {
    let policy = MemoryGcra::new(quota(50, 1));

    assert!(!check(&policy, "client-a").is_rate_limited());
    assert!(!check(&policy, "client-b").is_rate_limited());
    assert!(check(&policy, "client-a").is_rate_limited());
}

#[test]
fn clones_share_one_keyed_limiter_and_independent_constructors_do_not() {
    let quota = quota(50, 2);
    let first = MemoryGcra::new(quota);
    let cloned = first.clone();
    let independent = MemoryGcra::new(quota);

    assert!(!check(&first, "client").is_rate_limited());
    assert!(!check(&cloned, "client").is_rate_limited());
    assert!(check(&first, "client").is_rate_limited());
    assert!(!check(&independent, "client").is_rate_limited());
}

#[test]
fn inactive_keys_can_be_pruned_from_the_process_local_state() {
    let quota = quota(1_000, 1);
    let policy = MemoryGcra::new(quota);

    assert!(!check(&policy, "client").is_rate_limited());
    assert_eq!(policy.tracked_key_count(), 1);

    thread::sleep(quota.emission_interval() + Duration::from_millis(5));
    policy.prune_inactive_keys();

    assert_eq!(policy.tracked_key_count(), 0);
    assert!(!check(&policy, "client").is_rate_limited());
}

#[tokio::test]
async fn memory_gcra_decisions_drive_layer_fields_and_rejection() {
    let policy = MemoryGcra::new(quota(1, 1));
    let layer = RateLimitLayer::builder(StaticKey)
        .policy_name("memory-gcra")
        .with_policy(policy)
        .build()
        .expect("valid layer");
    let service = layer.layer(service_fn(|_request: Request<()>| async {
        Ok::<_, Infallible>(Response::new(()))
    }));

    let allowed = service
        .clone()
        .oneshot(Request::new(()))
        .await
        .expect("allowed response");
    assert_eq!(allowed.status(), StatusCode::OK);
    assert_eq!(
        allowed.headers().get("ratelimit-policy").expect("policy field"),
        "\"memory-gcra\";q=1;w=1"
    );
    assert_eq!(
        allowed.headers().get("ratelimit").expect("rate-limit field"),
        "\"memory-gcra\";r=0;t=1"
    );

    let rejected = service.oneshot(Request::new(())).await.expect("rate-limited response");
    assert_eq!(rejected.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(rejected.headers().get("retry-after").expect("retry field"), "1");
}
