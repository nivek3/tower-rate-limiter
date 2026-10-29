//! Concrete rate-limit storage adapters.

#[cfg(feature = "memory")]
mod memory;

#[cfg(feature = "memory")]
pub use memory::{MemoryStore, MemoryStoreError};

#[cfg(feature = "memory-gcra")]
mod memory_gcra;

#[cfg(feature = "memory-gcra")]
pub use memory_gcra::MemoryGcra;

#[cfg(all(feature = "redis", any(feature = "runtime-tokio", feature = "runtime-smol")))]
mod redis;

#[cfg(all(feature = "redis", any(feature = "runtime-tokio", feature = "runtime-smol")))]
pub use redis::{RedisStore, RedisStoreError};

#[cfg(all(feature = "redis-gcra", any(feature = "runtime-tokio", feature = "runtime-smol")))]
mod redis_gcra;

#[cfg(all(feature = "redis-gcra", any(feature = "runtime-tokio", feature = "runtime-smol")))]
pub use redis_gcra::RedisGcra;

#[cfg(feature = "postgres-gcra")]
mod postgres_gcra;

#[cfg(feature = "postgres-gcra")]
pub use postgres_gcra::{POSTGRES_GCRA_MIGRATION_V1, PostgresGcra};
