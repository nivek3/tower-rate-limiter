use std::{future::Future, time::Duration};

use tower_rate_limiter::{Decision, GcraQuota, RateLimitPolicy};

const TIMING_TOLERANCE: Duration = Duration::from_micros(1);

pub fn quotas() -> [GcraQuota; 2] {
    [
        GcraQuota::new(1, Duration::from_secs(1), 1).expect("burst-one quota"),
        GcraQuota::new(3, Duration::from_secs(1), 3).expect("non-divisible quota"),
    ]
}

fn assert_metadata(decision: Decision, quota: GcraQuota) {
    assert_eq!(decision.limit(), u64::from(quota.burst()));
    assert_eq!(decision.window(), quota.window());
    assert!(decision.remaining() < u64::from(quota.burst()));
    assert!(decision.reset_after() > Duration::ZERO);
    assert!(decision.reset_after() <= quota.window() + TIMING_TOLERANCE);
    if decision.is_rate_limited() {
        assert_eq!(decision.remaining(), 0);
        let retry = decision.retry_after().expect("rejection supplies retry-after");
        assert!(retry > Duration::ZERO);
        assert!(retry <= quota.emission_interval() + TIMING_TOLERANCE);
        assert!(retry <= decision.reset_after() + TIMING_TOLERANCE);
    } else {
        assert_eq!(decision.retry_after(), None);
    }
}

async fn consume_burst<P: RateLimitPolicy>(policy: &P, quota: GcraQuota) -> Decision {
    let mut previous_remaining = u64::from(quota.burst());
    for _ in 0..quota.burst() {
        let decision = policy.check("contract-client".into()).await.expect("charge burst");
        assert_metadata(decision, quota);
        assert!(!decision.is_rate_limited(), "fresh capacity: {quota:?}");
        assert!(decision.remaining() <= previous_remaining);
        previous_remaining = decision.remaining();
    }
    assert_eq!(previous_remaining, 0);
    let rejected = policy
        .check("contract-client".into())
        .await
        .expect("check exhausted burst");
    assert_metadata(rejected, quota);
    assert!(rejected.is_rate_limited());
    rejected
}

pub async fn assert_contract<P, S, Sleep>(policy: P, quota: GcraQuota, sleep: S)
where
    P: RateLimitPolicy,
    S: Fn(Duration) -> Sleep,
    Sleep: Future<Output = ()>,
{
    let first = consume_burst(&policy, quota).await;
    let isolated = policy.check("other-client".into()).await.expect("isolated key");
    assert_metadata(isolated, quota);
    assert!(!isolated.is_rate_limited());

    let repeated = policy
        .clone()
        .check("contract-client".into())
        .await
        .expect("repeated rejection");
    assert_metadata(repeated, quota);
    assert!(repeated.is_rate_limited());
    assert!(repeated.retry_after().expect("retry") <= first.retry_after().expect("retry") + TIMING_TOLERANCE);
    assert!(repeated.reset_after() <= first.reset_after() + TIMING_TOLERANCE);

    let margin = Duration::from_millis(50);
    sleep(repeated.retry_after().expect("retry") + margin).await;
    let recovered = policy.check("contract-client".into()).await.expect("retry recovery");
    assert_metadata(recovered, quota);
    assert!(!recovered.is_rate_limited());

    sleep(quota.window() + margin).await;
    consume_burst(&policy, quota).await;
}
