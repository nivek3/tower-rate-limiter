//! PostgreSQL-backed GCRA Policy, enabled by the `postgres-gcra` Cargo feature.

use std::{fmt, future::Future, pin::Pin, time::Duration};

use sqlx::{PgPool, Row};

use crate::{Decision, GcraQuota, RateLimitError, RateLimitPolicy};

const POLICY_ERROR_CODE: &str = "postgres_gcra_error";

/// Version 1 of the PostgreSQL GCRA migration installed by the application.
///
/// The adapter never executes this SQL automatically. Applications may pass this artifact to
/// their migration tooling or copy the packaged `migrations/postgres/0001_gcra.sql` file into the
/// application's own versioned migration directory.
pub const POSTGRES_GCRA_MIGRATION_V1: &str = include_str!("../../migrations/postgres/0001_gcra.sql");

/// A shared PostgreSQL-backed GCRA Policy using one validated static Quota.
///
/// The caller owns the pool lifecycle and must apply [`POSTGRES_GCRA_MIGRATION_V1`] before serving
/// traffic. Each charge calls the installed function; the adapter never performs DDL.
#[derive(Clone)]
pub struct PostgresGcra {
    pool: PgPool,
    quota: GcraQuota,
    namespace: Option<String>,
}

impl PostgresGcra {
    /// Construct a Policy from a caller-owned PostgreSQL pool and static Quota.
    pub const fn new(pool: PgPool, quota: GcraQuota) -> Self {
        Self {
            pool,
            quota,
            namespace: None,
        }
    }

    /// Add an optional PostgreSQL namespace. Empty namespaces are treated as absent.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Return the static Quota owned by this Policy.
    pub const fn quota(&self) -> GcraQuota {
        self.quota
    }

    fn namespace(&self) -> &str {
        self.namespace.as_deref().unwrap_or("")
    }
}

impl fmt::Debug for PostgresGcra {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresGcra")
            .field("quota", &self.quota)
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

impl RateLimitPolicy for PostgresGcra {
    type Future = Pin<Box<dyn Future<Output = Result<Decision, RateLimitError>> + Send>>;

    fn check(&self, key: String) -> Self::Future {
        let pool = self.pool.clone();
        let namespace = self.namespace().to_owned();
        let quota = self.quota;

        Box::pin(async move {
            let emission_interval_us = duration_to_microseconds(quota.emission_interval())?;
            let burst = i64::from(quota.burst());
            let row = sqlx::query(
                "SELECT allowed, remaining, retry_after_us, reset_after_us \
                 FROM tower_rate_limiter_gcra_v1.charge($1, $2, $3, $4)",
            )
            .bind(namespace)
            .bind(key)
            .bind(emission_interval_us)
            .bind(burst)
            .fetch_one(&pool)
            .await
            .map_err(PostgresGcraError::from)?;

            decision_from_row(&row, quota).map_err(Into::into)
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
enum PostgresGcraError {
    #[error("PostgreSQL GCRA query failed: {0}")]
    QueryFailed(String),
    #[error("PostgreSQL GCRA returned an invalid reply: {0}")]
    InvalidReply(&'static str),
}

impl From<sqlx::Error> for PostgresGcraError {
    fn from(error: sqlx::Error) -> Self {
        Self::QueryFailed(error.to_string())
    }
}

impl From<PostgresGcraError> for RateLimitError {
    fn from(error: PostgresGcraError) -> Self {
        Self::Policy(POLICY_ERROR_CODE.into(), error.to_string())
    }
}

fn duration_to_microseconds(duration: Duration) -> Result<i64, RateLimitError> {
    i64::try_from(duration.as_micros()).map_err(|_| {
        RateLimitError::Policy(
            POLICY_ERROR_CODE.into(),
            "GCRA emission interval exceeds PostgreSQL BIGINT microseconds".into(),
        )
    })
}

fn decision_from_row(row: &sqlx::postgres::PgRow, quota: GcraQuota) -> Result<Decision, PostgresGcraError> {
    let allowed: bool = row
        .try_get("allowed")
        .map_err(|_| PostgresGcraError::InvalidReply("allowed must be a boolean"))?;
    let remaining: i64 = row
        .try_get("remaining")
        .map_err(|_| PostgresGcraError::InvalidReply("remaining must be a BIGINT"))?;
    let retry_after_us: i64 = row
        .try_get("retry_after_us")
        .map_err(|_| PostgresGcraError::InvalidReply("retry-after must be a BIGINT"))?;
    let reset_after_us: i64 = row
        .try_get("reset_after_us")
        .map_err(|_| PostgresGcraError::InvalidReply("reset-after must be a BIGINT"))?;

    let burst = i64::from(quota.burst());
    if !(0..burst).contains(&remaining) {
        return Err(PostgresGcraError::InvalidReply(
            "remaining must be within the configured burst",
        ));
    }
    let reset_after = positive_duration(reset_after_us, "reset-after must be positive")?;
    if reset_after > quota.window() {
        return Err(PostgresGcraError::InvalidReply(
            "reset-after exceeds the configured window",
        ));
    }

    if allowed {
        if retry_after_us != 0 {
            return Err(PostgresGcraError::InvalidReply(
                "allowed reply must have zero retry-after",
            ));
        }
        let emission_interval_us = duration_to_microseconds(quota.emission_interval())
            .map_err(|_| PostgresGcraError::InvalidReply("configured emission interval is invalid"))?;
        let burst_window_us = emission_interval_us
            .checked_mul(burst)
            .ok_or(PostgresGcraError::InvalidReply("configured burst window is invalid"))?;
        let reset_after_us = i64::try_from(reset_after.as_micros())
            .map_err(|_| PostgresGcraError::InvalidReply("reset-after is out of range"))?;
        let expected_remaining = (burst_window_us - reset_after_us) / emission_interval_us;
        if remaining != expected_remaining {
            return Err(PostgresGcraError::InvalidReply(
                "allowed remaining does not match reset-after",
            ));
        }
        return Ok(Decision::allowed(
            u64::from(quota.burst()),
            quota.window(),
            u64::try_from(remaining).expect("remaining was validated non-negative"),
            reset_after,
        ));
    }

    if remaining != 0 {
        return Err(PostgresGcraError::InvalidReply(
            "rate-limited reply must have zero remaining",
        ));
    }
    let retry_after = positive_duration(retry_after_us, "retry-after must be positive")?;
    if retry_after > quota.emission_interval() {
        return Err(PostgresGcraError::InvalidReply(
            "retry-after exceeds one emission interval",
        ));
    }
    if retry_after > reset_after {
        return Err(PostgresGcraError::InvalidReply("retry-after exceeds reset-after"));
    }
    Ok(Decision::rate_limited(
        u64::from(quota.burst()),
        quota.window(),
        reset_after,
        retry_after,
    ))
}

fn positive_duration(microseconds: i64, error: &'static str) -> Result<Duration, PostgresGcraError> {
    let microseconds = u64::try_from(microseconds).map_err(|_| PostgresGcraError::InvalidReply(error))?;
    let duration = Duration::from_micros(microseconds);
    if duration.is_zero() {
        return Err(PostgresGcraError::InvalidReply(error));
    }
    Ok(duration)
}
