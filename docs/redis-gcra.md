# Redis GCRA

`RedisGcra` is the included shared policy for a static, continuously replenished quota. It works
with ordinary Redis scripting and does not require Redis Stack, `redis-cell`, or another Redis
module. The vendored artifact requires Redis 3.2 or newer because it uses
`redis.replicate_commands()`; this repository's live contract suite has been run against Redis
8.8, not a complete server-version matrix.

This page documents the Redis-specific operational contract. See [GCRA backends](gcra.md) for the
shared `GcraQuota` and `Decision` meanings, microsecond upward rounding, and namespace/versioning
rules that also apply to Memory and PostgreSQL GCRA.

## Setup

Enable it with one supported async runtime:

```toml
[dependencies]
redis = "1"
tower-rate-limiter = { version = "0.2", default-features = false, features = ["redis-gcra", "runtime-tokio"] }
```

For Smol, replace `runtime-tokio` with `runtime-smol`. The application owns Redis URL parsing,
connection creation, reconnect strategy, and shutdown; the policy accepts an established
`redis::aio::MultiplexedConnection`.

```rust
use std::{error::Error, time::Duration};
use tower_rate_limiter::{GcraQuota, IpKeyExtractor, RateLimitLayer, RedisGcra};

async fn configure() -> Result<RateLimitLayer<IpKeyExtractor, RedisGcra>, Box<dyn Error>> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let connection = client.get_multiplexed_async_connection().await?;

    let quota = GcraQuota::new(100, Duration::from_secs(60), 20)?;
    let layer = RateLimitLayer::builder(IpKeyExtractor::new())
        .policy_name("public-api")
        .with_policy(RedisGcra::new(connection, quota).with_namespace("my-service"))
        .build()?;
    Ok(layer)
}
```

`rate` requests replenish across `period`. `burst` is the total immediate capacity of a fresh key,
not an amount added to `rate`: the quota above admits at most 20 immediately, then replenishes at
100 requests per minute. This release always charges one request; it has no weighted-cost or refund
API. `GcraQuota` rounds the common emission interval up to whole microseconds; the complete burst
replenishment window must not exceed one year.

## Redis contract

Each check invokes an atomic Lua artifact and obtains time from Redis `TIME`; application-host clock
skew therefore does not set the GCRA decision. The ACL principal needs `EVALSHA`, `SCRIPT LOAD`,
`TIME`, `GET`, and `SET`. Script caching is internal to Redis; the application does not have to load
the script itself.

Set a namespace when applications or deployments share a Redis database. The namespace separates
Redis keys; a distinct `policy_name` still identifies a semantically distinct middleware policy.
Every replica charging the same resulting Redis key must use the same quota. When rolling out a
quota change, version the policy name or namespace so old and new instances do not reinterpret one
stored TAT with different emission intervals.

The bundled Lua file is a verbatim copy of the `allowN` artifact from
[`go-redis/redis_rate` v10.0.1](https://github.com/go-redis/redis_rate/tree/v10.0.1). It is based
on `rwz/redis-gcra`; source revision, checksum, and both license notices are retained in
[`THIRD_PARTY_NOTICES.md`](https://github.com/nivek-ph/tower-rate-limiter/blob/main/THIRD_PARTY_NOTICES.md).

## Failure and deployment limits

`PolicyFailureMode::Reject` is the default: a Redis command or reply-validation failure produces
the configured middleware rejection, normally `503`. Select `PolicyFailureMode::Allow` explicitly
only when short periods of under-enforcement are preferable to rejecting healthy requests; it calls
the inner service without rate-limit fields or context.

Redis asynchronous replication can lose recent limiter writes during failover, so a newly promoted
replica may briefly admit extra requests. This
release does not promise compatibility with Redis Cluster, Sentinel, or managed Redis proxies—validate
the exact topology, ACL, failover, and script behavior before production use.

## Precision

This policy deliberately follows the unmodified `go-redis/redis_rate` artifact. It uses Lua
floating-point seconds for time and TAT. Whole-microsecond `GcraQuota` rounding does not make that
arithmetic exact: at very small emission intervals, admission can be early or late, `remaining` can
be truncated, and a frozen-time burst can admit fewer **or more** than the configured capacity.
It does not guarantee strict microsecond enforcement or exact admission parity with Memory or
PostgreSQL.

Deterministic tests execute the pinned source with only `TIME` replaced and real Redis state
operations inside one atomic invocation. They cover 1µs, 10µs, and 100µs recovery boundaries.
Characterization vectors also reproduce 1006 allowed charges for a 3µs interval and `burst = 1000`
at Unix second 1800000000, and 1048 for a 1µs interval at Unix second 2200000000. These vectors
record upstream behavior; they are not a maximum-error bound or a recommended operating rate.

The upstream comment's epoch boundary around 2048-09-09 is not a guarantee of exact arithmetic
before that date. The Rust decoder's 1µs tolerance validates reply duration bounds; it does not
bound accumulated admission error. Treat response fields as pacing guidance and validate the
chosen quota under the application's workload when precision is important.
