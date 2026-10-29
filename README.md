# tower-rate-limiter

[Crates.io](https://crates.io/crates/tower-rate-limiter) ·
[API docs](https://docs.rs/tower-rate-limiter) ·
[Book](https://nivek-ph.github.io/tower-rate-limiter/) ·
[Examples](https://github.com/nivek-ph/tower-rate-limiter/tree/main/examples)

Keyed HTTP rate limiting middleware for [Tower](https://github.com/tower-rs/tower).

- Tower-first core with optional Axum integration
- Fixed-window policies plus Memory, Redis, and PostgreSQL GCRA for smooth quotas
- Custom policies, keys, responses, bypass rules, and policy-failure behavior
- IETF draft `RateLimit` and `RateLimit-Policy` response fields

## Quick start

The default feature includes the process-local `MemoryStore`. Rust 1.96 or newer is required.

```toml
[dependencies]
tower-rate-limiter = "0.2"
tower = "0.5"
```

Construct a fixed-window policy and layer it around a Tower service:

```rust
use std::time::Duration;
use tower::Layer;
use tower_rate_limiter::{FixedWindow, IpKeyExtractor, MemoryStore, RateLimitLayer};

let limiter = RateLimitLayer::builder(IpKeyExtractor::new())
    .policy_name("public-api")
    .with_policy(FixedWindow::new(MemoryStore::new(), 100, Duration::from_secs(60))?)
    .build()
    .expect("valid rate-limit policy");

let service = limiter.layer(inner_service);
```

This policy allows 100 requests per peer IP in each 60-second fixed window. The next request
receives `429 Too Many Requests` with `Retry-After`. See the
[quick-start guide](https://nivek-ph.github.io/tower-rate-limiter/getting-started.html) for a
complete runnable example.

> `MemoryStore` keeps fixed-window counters in one process. Use `RedisStore` with
> `FixedWindow`, or another shared `RateLimitPolicy`, when several application instances must
> enforce the same quota.

## GCRA backends

GCRA replenishes quota continuously rather than at a fixed-window boundary. `GcraQuota` is shared
by the included adapters: `rate` requests replenish over `period`, and `burst` is the total
immediate capacity of a fresh client key (not extra capacity added to `rate`). Every built-in GCRA
adapter charges exactly one request per check.

`GcraQuota` rounds its emission interval **up** to whole microseconds. That prevents the
microsecond-backed adapters from admitting faster than the configured long-term rate. The adapters
return the same `Decision` meanings—limit, window, remaining capacity, reset, and, after rejection,
the earliest retry—but do not promise byte-for-byte identical timing fields because they use
different clocks and state engines. See the [GCRA overview](https://nivek-ph.github.io/tower-rate-limiter/gcra.html).

### Memory

Use `MemoryGcra` for a single process. It delegates GCRA accounting to `governor`; cloned policies
share their in-process state, while separately constructed policies do not. It has no async runtime
or external-service dependency.

```toml
[dependencies]
tower-rate-limiter = { version = "0.2", default-features = false, features = ["memory-gcra"] }
```

```rust
use std::time::Duration;
use tower_rate_limiter::{GcraQuota, IpKeyExtractor, MemoryGcra, RateLimitLayer};

let quota = GcraQuota::new(100, Duration::from_secs(60), 20)?;
let limiter = RateLimitLayer::builder(IpKeyExtractor::new())
    .policy_name("public-api")
    .with_policy(MemoryGcra::new(quota))
    .build()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

### Redis

Use `RedisGcra` when replicas need one shared rate. It runs against ordinary Redis with scripting
enabled—no Redis Stack or `redis-cell` module is required. The pinned upstream Lua artifact itself
requires Redis 3.2 or newer.

```toml
[dependencies]
redis = "1"
tower-rate-limiter = { version = "0.2", default-features = false, features = ["redis-gcra", "runtime-tokio"] }
```

```rust,no_run
use std::time::Duration;
use tower_rate_limiter::{GcraQuota, IpKeyExtractor, RateLimitLayer, RedisGcra};

# async fn configure() -> Result<(), Box<dyn std::error::Error>> {
let client = redis::Client::open("redis://127.0.0.1/")?;
let connection = client.get_multiplexed_async_connection().await?;
let quota = GcraQuota::new(100, Duration::from_secs(60), 20)?;

let limiter = RateLimitLayer::builder(IpKeyExtractor::new())
    .policy_name("public-api")
    .with_policy(RedisGcra::new(connection, quota).with_namespace("my-service"))
    .build()?;
# Ok(())
# }
```

Use `runtime-smol` instead of `runtime-tokio` for Smol. The
[Redis GCRA guide](https://nivek-ph.github.io/tower-rate-limiter/redis-gcra.html) documents Redis
permissions, failure behavior, and operational limits.

### PostgreSQL

Use `PostgresGcra` when PostgreSQL is the shared state service. It uses a caller-owned `sqlx::PgPool`
and the application's PostgreSQL clock. Before traffic begins, apply
[`migrations/postgres/0001_gcra.sql`](migrations/postgres/0001_gcra.sql) with the application's
migration runner; the library never creates schema objects at runtime. This adapter is Tokio-only
through SQLx.

```toml
[dependencies]
sqlx = { version = "0.9", default-features = false, features = ["postgres", "runtime-tokio"] }
tower-rate-limiter = { version = "0.2", default-features = false, features = ["postgres-gcra"] }
```

```rust,no_run
use std::time::Duration;
use tower_rate_limiter::{GcraQuota, IpKeyExtractor, PostgresGcra, RateLimitLayer};

# async fn configure(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
let quota = GcraQuota::new(100, Duration::from_secs(60), 20)?;
let limiter = RateLimitLayer::builder(IpKeyExtractor::new())
    .policy_name("public-api-v2")
    .with_policy(PostgresGcra::new(pool, quota).with_namespace("my-service"))
    .build()?;
# let _ = limiter;
# Ok(())
# }
```

The [Memory GCRA](https://nivek-ph.github.io/tower-rate-limiter/memory-gcra.html),
[Redis GCRA](https://nivek-ph.github.io/tower-rate-limiter/redis-gcra.html), and
[PostgreSQL GCRA](https://nivek-ph.github.io/tower-rate-limiter/postgres-gcra.html) guides cover
their state, namespace, migration, permission, and cleanup contracts.

`IpKeyExtractor` reads the peer socket address from request extensions. When using forwarded
client-IP headers, define and enforce the trusted-proxy boundary first. See
[Axum and Redis](https://nivek-ph.github.io/tower-rate-limiter/adapters.html) and the
[production guide](https://nivek-ph.github.io/tower-rate-limiter/production.html).

## Documentation

- [Quick start](https://nivek-ph.github.io/tower-rate-limiter/getting-started.html)
- [Configuration](https://nivek-ph.github.io/tower-rate-limiter/configuration.html)
- [How it works](https://nivek-ph.github.io/tower-rate-limiter/concepts.html)
- [GCRA backends](https://nivek-ph.github.io/tower-rate-limiter/gcra.html)
- [Examples](https://nivek-ph.github.io/tower-rate-limiter/examples.html)
- [API documentation](https://docs.rs/tower-rate-limiter)

## Benchmarks

The repository includes a Docker Compose benchmark harness with isolated Redis, a benchmark server,
and an optional in-network `wrk` load generator. Results are written to `benchmarks/output/` with
raw measurements, `summary.csv`, and `report.txt`. See the
[benchmark guide](https://github.com/nivek-ph/tower-rate-limiter/blob/main/benchmarks/README.md)
for local runs, resource limits, and reproducible settings.

## Contributing

Bug reports, feature requests, and contributions are welcome through
[GitHub Issues](https://github.com/nivek-ph/tower-rate-limiter/issues).

## License

Licensed under either

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE)); or
- MIT License ([LICENSE-MIT](LICENSE-MIT)).

The vendored Redis GCRA artifact and its upstream notices are documented in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
