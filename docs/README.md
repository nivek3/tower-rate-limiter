# tower-rate-limiter

Keyed HTTP rate limiting middleware for Tower.

`tower-rate-limiter` lets an application decide **who** a request belongs to and **which policy**
charges it. The middleware owns the HTTP response flow without coupling the core to Axum, Tokio,
Redis, or a built-in identity policy.

```text
Request
  -> optional bypass
  -> KeyExtractor
  -> RateLimitPolicy::check
  -> allow the ready inner service or return a response
```

## When to use it

Use this crate when you need per-caller HTTP enforcement in a Tower service, for example:

- one quota per authenticated account or API client;
- an IP-based limit at an application boundary;
- route-specific policies composed as Tower Layers;
- fixed-window counters or GCRA quotas with process-local, Redis, or PostgreSQL state.

This is not a global concurrency limiter or a backpressure mechanism. The included policy is fixed
window; other algorithms are expressed as explicit policies.

## Design at a glance

| Concern | Application-facing seam | Included option |
| --- | --- | --- |
| Caller identity | `KeyExtractor` | `IpKeyExtractor`, `ClientIpKeyExtractor`, `TrustedProxyClientIpKeyExtractor` |
| Charging algorithm | `RateLimitPolicy` | `FixedWindow<S>`, `MemoryGcra`, `RedisGcra`, `PostgresGcra` |
| Fixed-window storage | `FixedWindowStore` | `MemoryStore`, optional `RedisStore` |
| Smooth quota configuration | `GcraQuota` | optional Memory, Redis, and PostgreSQL GCRA policies |
| Error and rejection responses | `ResponseFactory` | `DefaultResponseFactory` |

The policy is always explicit. This makes the charging algorithm and its state-ownership boundary
visible at layer construction instead of hiding process-local state behind a default singleton.

## Cargo features

| Feature | Default | Adds |
| --- | --- | --- |
| `memory` | yes | Runtime-independent `MemoryStore` for `FixedWindow` |
| `axum` | no | Axum `ConnectInfo<SocketAddr>` support in the built-in IP extractors |
| `redis` | no | `RedisStore` backed by an existing multiplexed connection for `FixedWindow` |
| `redis-lua` | no | Redis increment through Lua instead of `MULTI`/`EXEC` |
| `memory-gcra` | no | `MemoryGcra`: process-local, governor-backed one-request GCRA |
| `redis-gcra` | no | `RedisGcra`: shared one-request GCRA through an attributed Redis Lua artifact |
| `postgres-gcra` | no | `PostgresGcra`: shared one-request GCRA through caller-installed PostgreSQL SQL; SQLx Tokio runtime |
| `runtime-tokio` | no | Tokio-compatible Redis async runtime |
| `runtime-smol` | no | Smol-compatible Redis async runtime |
| `tracing` | no | Structured events for policy failures |

`RedisStore` needs an increment implementation (`redis` or `redis-lua`) together with one async
runtime (`runtime-tokio` or `runtime-smol`).

`MemoryGcra` needs `memory-gcra` only. `RedisGcra` needs `redis-gcra` together with one async
runtime and uses ordinary Redis, not a Redis module. `PostgresGcra` needs `postgres-gcra`; its SQLx
adapter is Tokio-only and requires an application-applied migration. See [GCRA backends](gcra.md)
for the shared contract and backend-specific deployment guides.

With `default-features = false`, the core remains usable with application-provided implementations
and does not pull in Axum, Redis, or an async runtime.

## Start here

1. Follow the [Quick start](getting-started.md) to construct a Tower layer.
2. Browse the [complete examples](examples.md) for progressively richer integrations.
3. Review [Configuration](configuration.md) before choosing policy names and failure mode.
4. Read [How it works](concepts.md) for exact charging semantics.
5. Use [Axum and Redis](adapters.md), choose a [GCRA backend](gcra.md), or implement [Custom components](custom-components.md).
6. Check the [Production guide](production.md) before deploying behind a proxy or across replicas.

The [API documentation](https://docs.rs/tower-rate-limiter) is the complete type-level reference.
The [GitHub repository](https://github.com/nivek-ph/tower-rate-limiter) contains runnable examples.
