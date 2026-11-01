//! Keyed HTTP rate limiting middleware for Tower.
//!
//! The crate is Tower-first: the core interfaces do not require Axum, Tokio, Redis, or a
//! particular request identity policy. Optional adapters are enabled through Cargo features.

mod limiter;
mod store;

#[cfg(any(feature = "memory-gcra", feature = "redis-gcra", feature = "postgres-gcra"))]
mod gcra;

pub use limiter::{
    ClientIpKeyExtractor, ConfigError, Decision, DecisionOutcome, DefaultResponseFactory, FixedWindow,
    FixedWindowFuture, FixedWindowStore, FixedWindowUsage, IpKeyExtractor, KeyExtractor, PolicyDecision,
    PolicyFailureMode, RateLimit, RateLimitBuilder, RateLimitContext, RateLimitError, RateLimitFields, RateLimitLayer,
    RateLimitPolicy, ResponseFactory, ResponseFuture, ResponseReason, TrustedProxyClientIpKeyExtractor,
};

#[cfg(any(feature = "memory-gcra", feature = "redis-gcra", feature = "postgres-gcra"))]
pub use gcra::{GcraQuota, GcraQuotaError};

#[cfg(feature = "memory")]
pub use store::{MemoryStore, MemoryStoreError};

#[cfg(feature = "memory-gcra")]
pub use store::MemoryGcra;

#[cfg(all(feature = "redis", any(feature = "runtime-tokio", feature = "runtime-smol")))]
pub use store::{RedisStore, RedisStoreError};

#[cfg(all(feature = "redis-gcra", any(feature = "runtime-tokio", feature = "runtime-smol")))]
pub use store::RedisGcra;

#[cfg(feature = "postgres-gcra")]
pub use store::{POSTGRES_GCRA_MIGRATION_V1, PostgresGcra};
