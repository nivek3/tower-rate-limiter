//! Redis rate-limit store, enabled by the `redis` Cargo feature.

use std::{future::Future, pin::Pin, time::Duration};

use redis::aio::MultiplexedConnection;

#[cfg(feature = "redis-lua")]
use redis::Script;

use crate::{FixedWindowStore, FixedWindowUsage, RateLimitError};

const REDIS_PREFIX: &str = "rl:";

#[cfg(feature = "redis-lua")]
const INCREMENT_SCRIPT: &str = r#"
local count = redis.call('INCR', KEYS[1])
if count == 1 then
    redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
return {count, redis.call('PTTL', KEYS[1])}
"#;

/// Errors returned by the Redis rate-limit store.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RedisStoreError {
    /// The fixed window cannot be represented in Redis milliseconds.
    #[error("window must be at least one millisecond")]
    WindowTooShort,

    /// The fixed window exceeds Redis's signed 64-bit millisecond range.
    #[error("window is too large")]
    WindowTooLarge,

    /// Redis rejected or could not execute the atomic increment operation.
    #[error("redis command failed: {0}")]
    CommandFailed(String),

    /// The atomic increment returned a non-positive usage count.
    #[error("redis returned invalid usage {0}")]
    InvalidUsage(i64),

    /// The atomic increment returned a non-positive reset duration.
    #[error("redis returned invalid reset-after milliseconds {0}")]
    InvalidResetAfter(i64),
}

impl From<redis::RedisError> for RedisStoreError {
    fn from(error: redis::RedisError) -> Self {
        Self::CommandFailed(error.to_string())
    }
}

/// Convert the RedisStoreError to a RateLimitError.
impl From<RedisStoreError> for RateLimitError {
    fn from(error: RedisStoreError) -> Self {
        RateLimitError::Policy("redis_store_error".into(), error.to_string())
    }
}

/// Redis-backed implementation of the common [`FixedWindowStore`] seam.
///
/// The connection is supplied by the caller and is cloned for each increment. A Redis
/// `MultiplexedConnection` clone shares its underlying connection and does not transfer
/// connection lifecycle ownership to this adapter. Window durations are truncated to whole
/// milliseconds because Redis expiry commands use millisecond precision; values shorter than one
/// millisecond are rejected.
#[derive(Clone, Debug)]
pub struct RedisStore {
    connection: MultiplexedConnection,
    namespace: Option<String>,
}

impl RedisStore {
    /// Construct a store from an established Redis multiplexed connection.
    pub fn new(connection: MultiplexedConnection) -> Self {
        Self {
            connection,
            namespace: None,
        }
    }

    /// Add an optional namespace. Empty namespaces are treated as absent.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Format the Redis transport key for this store's optional namespace.
    fn redis_key(&self, key: &str) -> String {
        format_redis_key(self.namespace.as_deref(), key)
    }
}

impl FixedWindowStore for RedisStore {
    type Future = Pin<Box<dyn Future<Output = Result<FixedWindowUsage, RateLimitError>> + Send>>;

    fn increment(&self, key: &str, window: Duration) -> Self::Future {
        let window_millis = match checked_window_millis(window) {
            Ok(window_millis) => window_millis,
            Err(error) => return Box::pin(std::future::ready(Err(error.into()))),
        };
        let redis_key = self.redis_key(key);
        let mut connection = self.connection.clone();

        Box::pin(async move {
            let result = increment_counter(&mut connection, &redis_key, window_millis).await?;
            usage_from_increment_result(result).map_err(Into::into)
        })
    }
}

#[cfg(feature = "redis-lua")]
async fn increment_counter(
    connection: &mut MultiplexedConnection,
    redis_key: &str,
    window_millis: i64,
) -> Result<(i64, i64), RedisStoreError> {
    let script = Script::new(INCREMENT_SCRIPT);
    let mut invocation = script.key(redis_key);
    invocation.arg(window_millis);
    invocation.invoke_async(connection).await.map_err(Into::into)
}

#[cfg(not(feature = "redis-lua"))]
async fn increment_counter(
    connection: &mut MultiplexedConnection,
    redis_key: &str,
    window_millis: i64,
) -> Result<(i64, i64), RedisStoreError> {
    redis::pipe()
        .atomic()
        .cmd("SET")
        .arg(redis_key)
        .arg(0)
        .arg("PX")
        .arg(window_millis)
        .arg("NX")
        .ignore()
        .cmd("INCR")
        .arg(redis_key)
        .cmd("PTTL")
        .arg(redis_key)
        .query_async(connection)
        .await
        .map_err(Into::into)
}

fn checked_window_millis(window: Duration) -> Result<i64, RedisStoreError> {
    let window_millis = window.as_millis();
    if window_millis == 0 {
        return Err(RedisStoreError::WindowTooShort);
    }
    if window_millis > i64::MAX as u128 {
        return Err(RedisStoreError::WindowTooLarge);
    }
    Ok(window_millis as i64)
}

/// Convert the atomic increment result `(count, PTTL milliseconds)` into [`FixedWindowUsage`].
///
/// Redis reports `-1` for a persistent key and `-2` for a missing key. Both, as well as a
/// zero TTL, are errors because the fixed window cannot be trusted without a positive TTL.
fn usage_from_increment_result((used, reset_after_millis): (i64, i64)) -> Result<FixedWindowUsage, RedisStoreError> {
    if used < 1 {
        return Err(RedisStoreError::InvalidUsage(used));
    }
    if reset_after_millis <= 0 {
        return Err(RedisStoreError::InvalidResetAfter(reset_after_millis));
    }

    Ok(FixedWindowUsage {
        used: used as u64,
        reset_after: Duration::from_millis(reset_after_millis as u64),
    })
}

/// Redis transport naming for a scoped Key already owned by the Rate Limiter.
fn format_redis_key(namespace: Option<&str>, key: &str) -> String {
    match namespace.filter(|value| !value.is_empty()) {
        Some(namespace) => format!("{namespace}:{REDIS_PREFIX}{key}"),
        None => format!("{REDIS_PREFIX}{key}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redis_store_implements_the_common_store_seam() {
        fn assert_store<T: FixedWindowStore>() {}
        assert_store::<RedisStore>();
    }

    #[test]
    fn increment_result_requires_positive_usage_and_ttl() {
        let usage = usage_from_increment_result((4, 1_500)).expect("valid Redis increment result");
        assert_eq!(
            usage,
            FixedWindowUsage {
                used: 4,
                reset_after: Duration::from_millis(1_500),
            }
        );

        assert!(matches!(
            usage_from_increment_result((0, 1_500)),
            Err(RedisStoreError::InvalidUsage(0))
        ));
        assert!(matches!(
            usage_from_increment_result((4, 0)),
            Err(RedisStoreError::InvalidResetAfter(0))
        ));
        assert!(matches!(
            usage_from_increment_result((4, -1)),
            Err(RedisStoreError::InvalidResetAfter(-1))
        ));
    }

    #[test]
    fn redis_transport_key_keeps_namespace_private_to_the_adapter() {
        assert_eq!(format_redis_key(None, "policy:client"), "rl:policy:client");
        assert_eq!(
            format_redis_key(Some("tenant"), "policy:client"),
            "tenant:rl:policy:client"
        );
        assert_eq!(format_redis_key(Some(""), "policy:client"), "rl:policy:client");
    }

    #[test]
    fn window_must_be_representable_as_positive_redis_milliseconds() {
        assert!(matches!(
            checked_window_millis(Duration::ZERO),
            Err(RedisStoreError::WindowTooShort)
        ));
        assert!(matches!(
            checked_window_millis(Duration::from_nanos(1)),
            Err(RedisStoreError::WindowTooShort)
        ));
        assert_eq!(
            checked_window_millis(Duration::from_millis(1)).expect("one millisecond"),
            1
        );
        assert_eq!(
            checked_window_millis(Duration::from_micros(1_500)).expect("sub-millisecond remainder is truncated"),
            1
        );
        assert_eq!(
            checked_window_millis(Duration::from_millis(i64::MAX as u64)),
            Ok(i64::MAX)
        );
        assert!(matches!(
            checked_window_millis(Duration::from_millis(i64::MAX as u64 + 1)),
            Err(RedisStoreError::WindowTooLarge)
        ));
    }

    #[test]
    fn redis_store_errors_map_to_one_store_error_code() {
        let error = RateLimitError::from(RedisStoreError::InvalidUsage(0));

        assert_eq!(
            error,
            RateLimitError::Policy(
                String::from("redis_store_error"),
                String::from("redis returned invalid usage 0"),
            )
        );
    }
}
