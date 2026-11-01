use std::{convert::Infallible, env, future::Future, time::Duration};

use http::{Request, Response, StatusCode};
use redis::aio::MultiplexedConnection;
use tower::{Layer, ServiceExt, service_fn};
use tower_rate_limiter::{
    GcraQuota, KeyExtractor, PolicyFailureMode, RateLimitError, RateLimitLayer, RateLimitPolicy, RedisGcra,
};

const CLIENT_KEY: &str = "client";

type ScriptReply = (i64, i64, String, String);

// Freeze only TIME; execute the pinned artifact and real Redis state changes in one atomic batch.
const FROZEN_TIME_SCRIPT: &str = concat!(
    r#"
local actual_redis = redis
local elapsed_us = 0
local redis = {
    replicate_commands = actual_redis.replicate_commands,
    call = function(command, ...)
        if command == "TIME" then
            return {tonumber(ARGV[5]) + math.floor(elapsed_us / 1000000), elapsed_us % 1000000}
        end
        return actual_redis.call(command, ...)
    end
}
local function charge()
"#,
    include_str!("../../src/store/redis_gcra_allow_n.lua"),
    r#"
end
local replies = {}
for i = 6, #ARGV do
    elapsed_us = tonumber(ARGV[i])
    replies[#replies + 1] = charge()
end
return replies
"#,
);

#[derive(Clone, Copy)]
struct StaticKey;

impl KeyExtractor for StaticKey {
    type Key = &'static str;

    fn extract<B>(&self, _request: &Request<B>) -> Result<Self::Key, RateLimitError> {
        Ok(CLIENT_KEY)
    }
}

async fn connection() -> MultiplexedConnection {
    let url = env::var("REDIS_URL").expect("REDIS_URL must point to the test Redis server");
    let client = redis::Client::open(url).expect("valid Redis URL");
    client
        .get_multiplexed_async_connection()
        .await
        .expect("connect to test Redis")
}

fn namespace(runtime: &str, test_name: &str) -> String {
    format!(
        "tower-rate-limiter-{runtime}-gcra:{test_name}:{}:{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after Unix epoch")
            .as_nanos()
    )
}

async fn policy(runtime: &str, test_name: &str, quota: GcraQuota) -> RedisGcra {
    RedisGcra::new(connection().await, quota).with_namespace(namespace(runtime, test_name))
}

async fn flush_script_cache() {
    let mut admin = connection().await;
    redis::cmd("SCRIPT")
        .arg("FLUSH")
        .query_async::<()>(&mut admin)
        .await
        .expect("flush script cache");
}

fn quota(rate: u32, burst: u32) -> GcraQuota {
    GcraQuota::new(rate, Duration::from_secs(1), burst).expect("valid GCRA quota")
}

async fn frozen_replies(runtime: &str, quota: GcraQuota, epoch: u64, offsets_us: &[u64]) -> Vec<ScriptReply> {
    let script = redis::Script::new(FROZEN_TIME_SCRIPT);
    let mut invocation = script.key(namespace(runtime, "frozen-time"));
    invocation
        .arg(quota.burst())
        .arg(1_u32)
        .arg(quota.emission_interval().as_secs_f64())
        .arg(1_u32)
        .arg(epoch)
        .arg(offsets_us);
    invocation
        .invoke_async(&mut connection().await)
        .await
        .expect("execute pinned script with controlled Redis time")
}

pub(crate) async fn upstream_microsecond_recovery_boundaries(runtime: &str) {
    for epoch in [1_800_000_000, 2_200_000_000, 2_483_228_799] {
        for interval_us in [1, 10, 100] {
            let quota = GcraQuota::new(1, Duration::from_micros(interval_us), 1).expect("microsecond quota");
            for offset in [interval_us - 1, interval_us, interval_us + 1] {
                let replies = frozen_replies(runtime, quota, epoch, &[0, offset]).await;
                assert_eq!(replies.len(), 2);
                assert_eq!(replies[0].0, 1);
                assert_eq!(
                    replies[1].0,
                    i64::from(offset >= interval_us),
                    "{epoch}, {interval_us}, {offset}"
                );
                for (allowed, remaining, retry, reset) in replies {
                    assert_eq!(remaining, 0);
                    let retry: f64 = retry.parse().expect("numeric retry");
                    let reset: f64 = reset.parse().expect("numeric reset");
                    assert!(reset > 0.0 && reset <= quota.window().as_secs_f64() + 0.000_001);
                    if allowed == 1 {
                        assert_eq!(retry, -1.0);
                    } else {
                        assert!(retry > 0.0 && retry <= reset);
                    }
                }
            }
        }
    }
}

pub(crate) async fn upstream_floating_point_burst_characterization(runtime: &str) {
    // These are upstream numerical behavior, not a strict burst-capacity guarantee.
    for (epoch, interval_us, expected_allowed) in [(1_800_000_000, 3, 1006), (2_200_000_000, 1, 1048)] {
        let quota = GcraQuota::new(1, Duration::from_micros(interval_us), 1000).expect("microsecond quota");
        let replies = frozen_replies(runtime, quota, epoch, &[0; 1100]).await;
        let allowed: i64 = replies.iter().map(|reply| reply.0).sum();
        assert_eq!(
            allowed, expected_allowed,
            "pinned upstream behavior at {epoch}, {interval_us}us"
        );
        assert!(allowed > i64::from(quota.burst()));
        assert_eq!(replies.last().expect("last charge").0, 0);
    }
}

pub(crate) async fn common_gcra_contract<S, Sleep>(runtime: &str, sleep: S)
where
    S: Fn(Duration) -> Sleep,
    Sleep: Future<Output = ()>,
{
    for quota in crate::gcra_contract::quotas() {
        let policy = policy(runtime, "common-contract", quota).await;
        crate::gcra_contract::assert_contract(policy, quota, &sleep).await;
    }
}

pub(crate) async fn burst_rejection_does_not_add_penalty_and_recovers<S, Sleep>(runtime: &str, sleep: S)
where
    S: FnOnce(Duration) -> Sleep,
    Sleep: Future<Output = ()>,
{
    let quota = quota(20, 3);
    let policy = policy(runtime, "burst-recovery", quota).await;
    let mut allowed = Vec::with_capacity(quota.burst() as usize);

    for _ in 0..3 {
        let decision = policy.check(CLIENT_KEY.into()).await.expect("charge fresh capacity");
        assert!(!decision.is_rate_limited());
        allowed.push(decision);
    }

    let mut previous_remaining = u64::from(quota.burst());
    for decision in &allowed {
        assert_eq!(decision.limit(), u64::from(quota.burst()));
        assert_eq!(decision.window(), quota.window());
        assert!(decision.remaining() < u64::from(quota.burst()));
        assert!(decision.remaining() <= previous_remaining);
        assert!(decision.reset_after() > Duration::ZERO);
        previous_remaining = decision.remaining();
    }
    assert_eq!(previous_remaining, 0);

    let first_rejection = policy.check(CLIENT_KEY.into()).await.expect("first rejected charge");
    assert!(first_rejection.is_rate_limited());
    let first_retry = first_rejection
        .retry_after()
        .expect("rejected decision has retry-after");
    assert_eq!(first_rejection.remaining(), 0);
    assert!(first_rejection.reset_after() > Duration::ZERO);
    assert!(first_retry > Duration::ZERO);

    let second_rejection = policy.check(CLIENT_KEY.into()).await.expect("second rejected charge");
    assert!(second_rejection.is_rate_limited());
    let second_retry = second_rejection
        .retry_after()
        .expect("rejected decision has retry-after");
    assert!(
        second_retry <= first_retry,
        "a rejected charge must not advance the GCRA penalty: first={first_retry:?}, second={second_retry:?}"
    );

    sleep(second_retry + Duration::from_millis(25)).await;
    assert!(
        !policy
            .check(CLIENT_KEY.into())
            .await
            .expect("charge after retry")
            .is_rate_limited()
    );
}

pub(crate) async fn separate_connections_share_one_quota(runtime: &str) {
    let shared_namespace = namespace(runtime, "shared-connections");
    let quota = quota(20, 2);
    let first = RedisGcra::new(connection().await, quota).with_namespace(shared_namespace.clone());
    let second = RedisGcra::new(connection().await, quota).with_namespace(shared_namespace);

    assert!(
        !first
            .check(CLIENT_KEY.into())
            .await
            .expect("first connection charge")
            .is_rate_limited()
    );
    assert!(
        !second
            .check(CLIENT_KEY.into())
            .await
            .expect("second connection charge")
            .is_rate_limited()
    );
    assert!(
        first
            .check(CLIENT_KEY.into())
            .await
            .expect("shared quota rejection")
            .is_rate_limited()
    );
}

pub(crate) async fn client_keys_are_isolated(runtime: &str) {
    let policy = policy(runtime, "key-isolation", quota(20, 1)).await;

    assert!(
        !policy
            .check("client-a".into())
            .await
            .expect("first client charge")
            .is_rate_limited()
    );
    assert!(
        !policy
            .check("client-b".into())
            .await
            .expect("second client charge")
            .is_rate_limited()
    );
    assert!(
        policy
            .check("client-a".into())
            .await
            .expect("first client is exhausted")
            .is_rate_limited()
    );
}

pub(crate) async fn concurrent_fresh_charges_allow_exactly_the_burst<Run, Runs>(runtime: &str, run: Run)
where
    Run: FnOnce(RedisGcra, usize) -> Runs,
    Runs: Future<Output = usize>,
{
    const BURST: usize = 8;

    let policy = policy(runtime, "concurrent-burst", quota(20, BURST as u32)).await;
    let allowed = run(policy, BURST * 3).await;

    assert_eq!(allowed, BURST);
}

pub(crate) async fn script_flush_recovers_on_the_next_charge(runtime: &str) {
    let policy = policy(runtime, "script-flush", quota(20, 2)).await;
    assert!(
        !policy
            .check("before-flush".into())
            .await
            .expect("load and invoke script")
            .is_rate_limited()
    );

    flush_script_cache().await;

    assert!(
        !policy
            .check("after-flush".into())
            .await
            .expect("reload script after NOSCRIPT")
            .is_rate_limited()
    );
}

pub(crate) async fn corrupt_redis_state_is_a_policy_error(runtime: &str) {
    let quota = quota(20, 1);
    let namespace = namespace(runtime, "corrupt-state");
    let policy = RedisGcra::new(connection().await, quota).with_namespace(namespace.clone());
    let redis_key = format!("{namespace}:rl:gcra:{CLIENT_KEY}");

    let mut writer = connection().await;
    redis::cmd("SET")
        .arg(redis_key)
        .arg("not-a-gcra-tat")
        .query_async::<()>(&mut writer)
        .await
        .expect("write corrupt state in isolated test namespace");

    let error = policy
        .check(CLIENT_KEY.into())
        .await
        .expect_err("corrupt state must not become a rate-limited decision");
    assert!(matches!(error, RateLimitError::Policy(_, _)));
    assert_eq!(error.code(), "redis_gcra_error");
}

pub(crate) async fn redis_policy_errors_follow_layer_failure_mode(runtime: &str) {
    let quota = quota(20, 1);
    let namespace = namespace(runtime, "layer-failure-mode");
    let policy_name = "redis-gcra";
    let policy = RedisGcra::new(connection().await, quota).with_namespace(namespace.clone());
    let redis_key = format!("{namespace}:rl:gcra:{policy_name}:{CLIENT_KEY}");

    let mut writer = connection().await;
    redis::cmd("SET")
        .arg(redis_key)
        .arg("not-a-gcra-tat")
        .query_async::<()>(&mut writer)
        .await
        .expect("write corrupt state for the Layer seam");

    let inner = || service_fn(|_request: Request<()>| async { Ok::<_, Infallible>(Response::new(())) });
    let rejected = RateLimitLayer::builder(StaticKey)
        .policy_name(policy_name)
        .with_policy(policy.clone())
        .build()
        .expect("rejecting layer")
        .layer(inner())
        .oneshot(Request::new(()))
        .await
        .expect("middleware response");
    assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(rejected.headers().get("ratelimit").is_none());
    assert!(rejected.headers().get("ratelimit-policy").is_none());
    assert!(rejected.headers().get("retry-after").is_none());

    let allowed = RateLimitLayer::builder(StaticKey)
        .policy_name(policy_name)
        .policy_failure_mode(PolicyFailureMode::Allow)
        .with_policy(policy)
        .build()
        .expect("fail-open layer")
        .layer(inner())
        .oneshot(Request::new(()))
        .await
        .expect("inner response");
    assert_eq!(allowed.status(), StatusCode::OK);
    assert!(allowed.headers().get("ratelimit").is_none());
    assert!(allowed.headers().get("ratelimit-policy").is_none());
    assert!(allowed.headers().get("retry-after").is_none());
}
