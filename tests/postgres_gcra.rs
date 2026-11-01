use std::{
    env,
    sync::{Arc, OnceLock},
    time::Duration,
};

use sqlx::{PgPool, Row};
use tower_rate_limiter::{GcraQuota, PostgresGcra, RateLimitError, RateLimitPolicy};

mod gcra_contract;

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

#[tokio::test]
async fn common_gcra_contract() {
    let _guard = TEST_LOCK.lock().await;
    let Some(pool) = pool().await else {
        return;
    };
    for quota in gcra_contract::quotas() {
        let policy = PostgresGcra::new(pool.clone(), quota).with_namespace(namespace("common-contract"));
        gcra_contract::assert_contract(policy, quota, tokio::time::sleep).await;
    }
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
async fn cleanup_expiry_rounds_up_without_rounding_tat_or_reset() {
    let _guard = TEST_LOCK.lock().await;
    let Some(pool) = pool().await else {
        return;
    };
    let namespace = namespace("expiry-rounding");
    let quota = GcraQuota::new(8, Duration::from_secs(1), 1_000).expect("valid rounding-test quota");
    let emission_us = i64::try_from(quota.emission_interval().as_micros()).expect("interval fits BIGINT");
    let policy = PostgresGcra::new(pool.clone(), quota).with_namespace(namespace.clone());
    let clock_sql = "SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::BIGINT";
    let now_us: i64 = sqlx::query_scalar(clock_sql)
        .fetch_one(&pool)
        .await
        .expect("read database time");
    let target_second_us = (now_us / 1_000_000 + 60) * 1_000_000;

    for offset_us in [0_i64, 1, 999_999] {
        let target_tat_us = target_second_us + offset_us;
        let key = format!("boundary-{offset_us}");
        sqlx::query(
            "INSERT INTO tower_rate_limiter_gcra_v1.state (namespace, client_key, tat_us, expires_at) \
             VALUES ($1, $2, $3, TIMESTAMPTZ 'infinity')",
        )
        .bind(&namespace)
        .bind(&key)
        .bind(target_tat_us - emission_us)
        .execute(&pool)
        .await
        .expect("seed future TAT for a deterministic second boundary");

        let before_us: i64 = sqlx::query_scalar(clock_sql)
            .fetch_one(&pool)
            .await
            .expect("read time before charge");
        let decision = policy.check(key.clone()).await.expect("charge seeded state");
        assert!(!decision.is_rate_limited());

        let row = sqlx::query(
            "SELECT tat_us, (EXTRACT(EPOCH FROM expires_at) * 1000000)::BIGINT AS expiry_us, \
             (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::BIGINT AS now_us \
             FROM tower_rate_limiter_gcra_v1.state WHERE namespace = $1 AND client_key = $2",
        )
        .bind(&namespace)
        .bind(&key)
        .fetch_one(&pool)
        .await
        .expect("read precise TAT and rounded cleanup expiry");
        let tat_us: i64 = row.try_get("tat_us").expect("TAT is BIGINT");
        let expiry_us: i64 = row.try_get("expiry_us").expect("expiry is BIGINT microseconds");
        let after_us: i64 = row.try_get("now_us").expect("database time is BIGINT microseconds");
        assert_eq!(tat_us, target_tat_us, "TAT must keep its microsecond precision");
        let expected_expiry_us = target_second_us + if offset_us == 0 { 0 } else { 1_000_000 };
        assert_eq!(expiry_us, expected_expiry_us);
        assert!((0..1_000_000).contains(&(expiry_us - tat_us)));

        let reset_us = i64::try_from(decision.reset_after().as_micros()).expect("reset fits BIGINT");
        assert!(
            (target_tat_us - after_us..=target_tat_us - before_us).contains(&reset_us),
            "reset must use precise TAT rather than rounded cleanup expiry"
        );
    }
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
    let start = Arc::new(tokio::sync::Barrier::new(25));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let policy = policy.clone();
        let start = Arc::clone(&start);
        tasks.spawn(async move {
            start.wait().await;
            !policy
                .check("client".into())
                .await
                .expect("concurrent charge")
                .is_rate_limited()
        });
    }
    start.wait().await;

    let mut allowed = 0;
    while let Some(result) = tasks.join_next().await {
        if result.expect("task") {
            allowed += 1;
        }
    }
    assert_eq!(allowed, 8);
}

#[tokio::test]
async fn charge_recreates_state_deleted_while_waiting_for_the_row_lock() {
    let _guard = TEST_LOCK.lock().await;
    let Some(pool) = pool().await else {
        return;
    };
    let namespace = namespace("cleanup-race");
    sqlx::query(
        "INSERT INTO tower_rate_limiter_gcra_v1.state (namespace, client_key, tat_us, expires_at) \
         VALUES ($1, 'client', 0, TIMESTAMPTZ 'epoch')",
    )
    .bind(&namespace)
    .execute(&pool)
    .await
    .expect("seed expired state");

    let mut cleanup = pool.begin().await.expect("begin cleanup");
    let deleted = sqlx::query(
        "DELETE FROM tower_rate_limiter_gcra_v1.state \
         WHERE namespace = $1 AND client_key = 'client' AND expires_at <= statement_timestamp()",
    )
    .bind(&namespace)
    .execute(&mut *cleanup)
    .await
    .expect("delete expired state without committing yet");
    assert_eq!(deleted.rows_affected(), 1);

    let charge_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(postgres_url().expect("POSTGRES_URL was established"))
        .await
        .expect("connect independent charge pool");
    let charge_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&charge_pool)
        .await
        .expect("read charge connection PID");
    let slow_quota = GcraQuota::new(1, Duration::from_secs(60), 2).expect("stable cleanup-race quota");
    let policy = PostgresGcra::new(charge_pool, slow_quota).with_namespace(namespace);
    let check_policy = policy.clone();
    let check = tokio::spawn(async move { check_policy.check("client".into()).await });

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT cardinality(pg_blocking_pids($1)) > 0")
                .bind(charge_pid)
                .fetch_one(&mut *cleanup)
                .await
                .expect("observe charge waiting for cleanup");
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("charge must wait until cleanup commits");
    cleanup.commit().await.expect("commit cleanup");

    let decision = tokio::time::timeout(Duration::from_secs(5), check)
        .await
        .expect("charge must finish after cleanup commits")
        .expect("charge task")
        .expect("charge recreated state");
    assert!(!decision.is_rate_limited());
    assert_eq!(decision.remaining(), 1);
    assert!(
        !policy
            .check("client".into())
            .await
            .expect("charge remaining capacity")
            .is_rate_limited()
    );
    assert!(
        policy
            .check("client".into())
            .await
            .expect("recreated state must enforce the quota")
            .is_rate_limited()
    );
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
