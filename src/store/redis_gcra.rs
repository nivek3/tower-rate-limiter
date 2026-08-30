//! Redis-backed GCRA Policy, enabled by the `redis-gcra` Cargo feature.

use std::{fmt, future::Future, pin::Pin, sync::LazyLock, time::Duration};

use redis::{Script, Value, aio::MultiplexedConnection};

use crate::{Decision, GcraQuota, RateLimitError, RateLimitPolicy};

const REDIS_PREFIX: &str = "rl:gcra:";
const REPLY_TOLERANCE: Duration = Duration::from_micros(1);
const ALLOW_ONE_SCRIPT_SOURCE: &str = include_str!("redis_gcra_allow_n.lua");

static ALLOW_ONE_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(ALLOW_ONE_SCRIPT_SOURCE));

/// Internal failures while charging a Redis GCRA Policy.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
enum RedisGcraError {
    /// Redis rejected or could not execute the upstream Lua artifact.
    #[error("Redis GCRA command failed: {0}")]
    CommandFailed(String),

    /// Redis returned a value that cannot be trusted as a GCRA Decision.
    #[error("Redis GCRA returned an invalid reply: {0}")]
    InvalidReply(&'static str),
}

impl From<redis::RedisError> for RedisGcraError {
    fn from(error: redis::RedisError) -> Self {
        Self::CommandFailed(error.to_string())
    }
}

impl From<RedisGcraError> for RateLimitError {
    fn from(error: RedisGcraError) -> Self {
        Self::Policy("redis_gcra_error".into(), error.to_string())
    }
}

/// A shared Redis-backed GCRA Policy using one validated static Quota.
///
/// The caller owns connection lifecycle and supplies an established redis-rs multiplexed
/// connection. Each charge uses Redis server time and one atomic invocation of the attributed
/// upstream Lua artifact. The minimal Policy always charges exactly one request.
#[derive(Clone)]
pub struct RedisGcra {
    connection: MultiplexedConnection,
    quota: GcraQuota,
    namespace: Option<String>,
}

impl RedisGcra {
    /// Construct a Policy from an established Redis multiplexed connection and static Quota.
    pub const fn new(connection: MultiplexedConnection, quota: GcraQuota) -> Self {
        Self {
            connection,
            quota,
            namespace: None,
        }
    }

    /// Add an optional Redis key namespace. Empty namespaces are treated as absent.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Return the static Quota owned by this Policy.
    pub const fn quota(&self) -> GcraQuota {
        self.quota
    }

    fn redis_key(&self, key: &str) -> String {
        format_redis_key(self.namespace.as_deref(), key)
    }
}

impl fmt::Debug for RedisGcra {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisGcra")
            .field("quota", &self.quota)
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

impl RateLimitPolicy for RedisGcra {
    type Future = Pin<Box<dyn Future<Output = Result<Decision, RateLimitError>> + Send>>;

    fn check(&self, key: String) -> Self::Future {
        let redis_key = self.redis_key(&key);
        let quota = self.quota;
        let mut connection = self.connection.clone();

        Box::pin(async move {
            let mut invocation = ALLOW_ONE_SCRIPT.key(redis_key);
            invocation
                .arg(quota.burst())
                .arg(1_u32)
                .arg(quota.emission_interval().as_secs_f64())
                .arg(1_u32);
            let reply = invocation
                .invoke_async::<Value>(&mut connection)
                .await
                .map_err(RedisGcraError::from)?;

            decision_from_reply(reply, quota).map_err(Into::into)
        })
    }
}

fn decision_from_reply(reply: Value, quota: GcraQuota) -> Result<Decision, RedisGcraError> {
    let Value::Array(values) = reply else {
        return Err(RedisGcraError::InvalidReply("expected a four-element array"));
    };
    let [allowed, remaining, retry_after, reset_after]: [Value; 4] = values
        .try_into()
        .map_err(|_| RedisGcraError::InvalidReply("expected a four-element array"))?;

    let Value::Int(allowed) = allowed else {
        return Err(RedisGcraError::InvalidReply("allowed must be an integer"));
    };
    if allowed != 0 && allowed != 1 {
        return Err(RedisGcraError::InvalidReply("allowed must be zero or one"));
    }

    let Value::Int(remaining) = remaining else {
        return Err(RedisGcraError::InvalidReply("remaining must be an integer"));
    };
    let remaining =
        u32::try_from(remaining).map_err(|_| RedisGcraError::InvalidReply("remaining must be non-negative"))?;
    if remaining > quota.burst() {
        return Err(RedisGcraError::InvalidReply("remaining exceeds burst"));
    }

    let retry_after = seconds_string(retry_after, "retry-after must be a finite string")?;
    let reset_after = seconds_string(reset_after, "reset-after must be a finite string")?;
    let reset_after = non_negative_duration(reset_after, "reset-after must be non-negative")?;
    if reset_after.is_zero() {
        return Err(RedisGcraError::InvalidReply("reset-after must be positive"));
    }
    if reset_after > quota.window().saturating_add(REPLY_TOLERANCE) {
        return Err(RedisGcraError::InvalidReply(
            "reset-after exceeds the configured window",
        ));
    }

    match allowed {
        1 => {
            if retry_after != -1.0 {
                return Err(RedisGcraError::InvalidReply(
                    "allowed reply must use the negative-one retry sentinel",
                ));
            }
            if remaining >= quota.burst() {
                return Err(RedisGcraError::InvalidReply(
                    "allowed remaining must be less than burst",
                ));
            }
            Ok(Decision::allowed(
                u64::from(quota.burst()),
                quota.window(),
                u64::from(remaining),
                reset_after,
            ))
        },
        0 => {
            if remaining != 0 {
                return Err(RedisGcraError::InvalidReply(
                    "rate-limited reply must have zero remaining",
                ));
            }
            if retry_after <= 0.0 {
                return Err(RedisGcraError::InvalidReply(
                    "rate-limited reply must have positive retry-after",
                ));
            }
            let retry_after = non_negative_duration(retry_after, "retry-after is out of range")?;
            if retry_after > quota.emission_interval().saturating_add(REPLY_TOLERANCE) {
                return Err(RedisGcraError::InvalidReply(
                    "retry-after exceeds one emission interval",
                ));
            }
            if retry_after > reset_after.saturating_add(REPLY_TOLERANCE) {
                return Err(RedisGcraError::InvalidReply("retry-after exceeds reset-after"));
            }
            Ok(Decision::rate_limited(
                u64::from(quota.burst()),
                quota.window(),
                reset_after,
                retry_after,
            ))
        },
        _ => unreachable!("allowed was validated above"),
    }
}

fn seconds_string(value: Value, error: &'static str) -> Result<f64, RedisGcraError> {
    let Value::BulkString(bytes) = value else {
        return Err(RedisGcraError::InvalidReply(error));
    };
    let value = std::str::from_utf8(&bytes)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .ok_or(RedisGcraError::InvalidReply(error))?;
    Ok(value)
}

fn non_negative_duration(value: f64, error: &'static str) -> Result<Duration, RedisGcraError> {
    if value < 0.0 {
        return Err(RedisGcraError::InvalidReply(error));
    }
    Duration::try_from_secs_f64(value).map_err(|_| RedisGcraError::InvalidReply(error))
}

fn format_redis_key(namespace: Option<&str>, key: &str) -> String {
    match namespace.filter(|namespace| !namespace.is_empty()) {
        Some(namespace) => format!("{namespace}:{REDIS_PREFIX}{key}"),
        None => format!("{REDIS_PREFIX}{key}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quota() -> GcraQuota {
        GcraQuota::new(10, Duration::from_secs(1), 4).expect("valid quota")
    }

    fn seconds(value: &str) -> Value {
        Value::BulkString(value.as_bytes().to_vec())
    }

    #[test]
    fn vendored_artifact_matches_the_pinned_redis_script_hash() {
        assert_eq!(ALLOW_ONE_SCRIPT.get_hash(), "f52a5b566060d79fd321d695df55bc918339c4aa");
    }

    #[test]
    fn valid_replies_become_algorithm_independent_decisions() {
        let allowed = decision_from_reply(
            Value::Array(vec![Value::Int(1), Value::Int(3), seconds("-1"), seconds("0.4")]),
            quota(),
        )
        .expect("allowed decision");
        assert_eq!(
            allowed,
            Decision::allowed(4, Duration::from_millis(400), 3, Duration::from_millis(400))
        );

        let rejected = decision_from_reply(
            Value::Array(vec![Value::Int(0), Value::Int(0), seconds("0.02"), seconds("0.3")]),
            quota(),
        )
        .expect("rate-limited decision");
        assert_eq!(
            rejected,
            Decision::rate_limited(
                4,
                Duration::from_millis(400),
                Duration::from_millis(300),
                Duration::from_millis(20),
            )
        );
    }

    #[test]
    fn malformed_replies_are_policy_errors() {
        assert!(matches!(
            decision_from_reply(Value::Int(1), quota()),
            Err(RedisGcraError::InvalidReply("expected a four-element array"))
        ));
        assert!(matches!(
            decision_from_reply(
                Value::Array(vec![Value::Int(1), Value::Int(5), seconds("-1"), seconds("0.4")]),
                quota(),
            ),
            Err(RedisGcraError::InvalidReply("remaining exceeds burst"))
        ));
        assert!(matches!(
            decision_from_reply(
                Value::Array(vec![Value::Int(0), Value::Int(0), seconds("nan"), seconds("0.4")]),
                quota(),
            ),
            Err(RedisGcraError::InvalidReply("retry-after must be a finite string"))
        ));
        assert!(matches!(
            decision_from_reply(
                Value::Array(vec![Value::Int(0), Value::Int(0), seconds("0.1"), seconds("0")]),
                quota(),
            ),
            Err(RedisGcraError::InvalidReply("reset-after must be positive"))
        ));
        assert!(matches!(
            decision_from_reply(
                Value::Array(vec![Value::Int(0), Value::Int(0), seconds("0.05"), seconds("0.01")]),
                quota(),
            ),
            Err(RedisGcraError::InvalidReply("retry-after exceeds reset-after"))
        ));
    }

    #[test]
    fn transport_keys_keep_gcra_state_separate() {
        assert_eq!(format_redis_key(None, "policy:client"), "rl:gcra:policy:client");
        assert_eq!(
            format_redis_key(Some("tenant"), "policy:client"),
            "tenant:rl:gcra:policy:client"
        );
        assert_eq!(format_redis_key(Some(""), "policy:client"), "rl:gcra:policy:client");
    }
}
