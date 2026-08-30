use std::{env, sync::OnceLock, time::Duration};

use sqlx::{PgPool, Row};
use tower_rate_limiter::{GcraQuota, PostgresGcra, RateLimitError, RateLimitPolicy};

static MIGRATION: &str = include_str!("../migrations/postgres/0001_gcra.sql");
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static POSTGRES_URL: OnceLock<Option<String>> = OnceLock::new();

fn postgres_url() -> Option<&'static str> {
    POSTGRES_URL.get_or_init(|| env::var("POSTGRES_URL").ok()).as_deref()
}

async fn pool() -> Option<PgPool> {
    let url = postgres_url()?;
    let pool = PgPool::connect(url).await.expect("connect to POSTGRES_URL");
    sqlx::raw_sql(MIGRATION)
        .execute(&pool)
        .await
        .expect("apply GCRA migration to isolated test database");
    Some(pool)
}

fn namespace(test_name: &str) -> String {
    format!(
        "tower-rate-limiter-postgres-gcra:{test_name}:{}:{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after Unix epoch")
            .as_nanos()
    )
}

fn quota(rate: u32, burst: u32) -> GcraQuota {
    GcraQuota::new(rate, Duration::from_secs(1), burst).expect("valid GCRA quota")
}

async fn expires_at_us(pool: &PgPool, namespace: &str, key: &str) -> i64 {
    sqlx::query(
        "SELECT (EXTRACT(EPOCH FROM expires_at) * 1000000)::BIGINT AS expires_at_us \
         FROM tower_rate_limiter_gcra_v1.state WHERE namespace = $1 AND client_key = $2",
    )
    .bind(namespace)
    .bind(key)
    .fetch_one(pool)
    .await
    .expect("read isolated GCRA expiry")
    .try_get("expires_at_us")
    .expect("expiry is a BIGINT microsecond value")
}

#[tokio::test]
async fn burst_rejection_does_not_add_penalty_and_recovers() {
    let _guard = TEST_LOCK.lock().await;
    let Some(pool) = pool().await else {
        return;
    };
    let quota = quota(20, 3);
    let namespace = namespace("burst-recovery");
    let policy = PostgresGcra::new(pool.clone(), quota).with_namespace(namespace.clone());

    for remaining in [2, 1, 0] {
        let decision = policy.check("client".into()).await.expect("allowed charge");
        assert!(!decision.is_rate_limited());
        assert_eq!(decision.remaining(), remaining);
        assert!(decision.reset_after() > Duration::ZERO);
    }

    let first = policy.check("client".into()).await.expect("first rejection");
    assert!(first.is_rate_limited());
    let retry = first.retry_after().expect("retry-after");
    assert!(retry > Duration::ZERO);
    let expiry_after_first_rejection = expires_at_us(&pool, &namespace, "client").await;

    tokio::time::sleep(Duration::from_millis(5)).await;
    let second = policy.check("client".into()).await.expect("second rejection");
    assert!(second.is_rate_limited());
    assert!(second.retry_after().expect("retry-after") <= retry);
    assert_eq!(
        expires_at_us(&pool, &namespace, "client").await,
        expiry_after_first_rejection,
        "a rejected charge must not extend state expiry"
    );

    tokio::time::sleep(retry + Duration::from_millis(25)).await;
    assert!(
        !policy
            .check("client".into())
            .await
            .expect("recovered charge")
            .is_rate_limited()
    );
}

#[tokio::test]
async fn independent_pools_share_and_keys_are_isolated() {
    let _guard = TEST_LOCK.lock().await;
    let Some(first_pool) = pool().await else {
        return;
    };
    let second_pool = pool().await.expect("POSTGRES_URL was established");
    let quota = quota(20, 2);
    let namespace = namespace("shared-pools");
    let first = PostgresGcra::new(first_pool, quota).with_namespace(namespace.clone());
    let second = PostgresGcra::new(second_pool, quota).with_namespace(namespace);

    assert!(
        !first
            .check("same".into())
            .await
            .expect("first charge")
            .is_rate_limited()
    );
    assert!(
        !second
            .check("same".into())
            .await
            .expect("second charge")
            .is_rate_limited()
    );
    assert!(
        first
            .check("same".into())
            .await
            .expect("shared rejection")
            .is_rate_limited()
    );
    assert!(
        !first
            .check("other".into())
            .await
            .expect("isolated key")
            .is_rate_limited()
    );
}

#[tokio::test]
async fn concurrent_fresh_charges_allow_exactly_the_burst() {
    let _guard = TEST_LOCK.lock().await;
    let Some(pool) = pool().await else {
        return;
    };
    let slow_quota = GcraQuota::new(1, Duration::from_secs(60), 8).expect("stable concurrency quota");
    let policy = PostgresGcra::new(pool, slow_quota).with_namespace(namespace("concurrency"));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let policy = policy.clone();
        tasks.spawn(async move {
            !policy
                .check("client".into())
                .await
                .expect("concurrent charge")
                .is_rate_limited()
        });
    }

    let mut allowed = 0;
    while let Some(result) = tasks.join_next().await {
        if result.expect("task") {
            allowed += 1;
        }
    }
    assert_eq!(allowed, 8);
}

#[tokio::test]
async fn corrupt_state_is_a_policy_error() {
    let _guard = TEST_LOCK.lock().await;
    let Some(pool) = pool().await else {
        return;
    };
    let quota = quota(20, 1);
    let namespace = namespace("corrupt-state");
    let policy = PostgresGcra::new(pool.clone(), quota).with_namespace(namespace.clone());

    sqlx::query(
        "INSERT INTO tower_rate_limiter_gcra_v1.state (namespace, client_key, tat_us, expires_at) \
         VALUES ($1, $2, -1, clock_timestamp())",
    )
    .bind(&namespace)
    .bind("corrupt")
    .execute(&pool)
    .await
    .expect("write corrupt state in isolated namespace");
    let error = policy
        .check("corrupt".into())
        .await
        .expect_err("corrupt state must fail");
    assert!(matches!(error, RateLimitError::Policy(_, _)));
    assert_eq!(error.code(), "postgres_gcra_error");
}
