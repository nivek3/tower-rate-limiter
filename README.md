# tower-rate-limiter

[Crates.io](https://crates.io/crates/tower-rate-limiter) ·
[API docs](https://docs.rs/tower-rate-limiter) ·
[Book](https://nivek-ph.github.io/tower-rate-limiter/) ·
[Examples](https://github.com/nivek-ph/tower-rate-limiter/tree/main/examples)

Keyed, fixed-window HTTP rate limiting middleware for [Tower](https://github.com/tower-rs/tower).

- Tower-first core with optional Axum integration
- Fixed or request-derived quotas
- Process-local memory and shared Redis counters
- Custom keys, responses, bypass rules, and Store failure behavior
- IETF draft `RateLimit` and `RateLimit-Policy` response fields

## Quick start

The default feature includes the process-local `MemoryStore`. Rust 1.96 or newer is required.

```toml
[dependencies]
tower-rate-limiter = "0.1"
tower = "0.5"
```

Create a policy, attach a Store, and layer it around a Tower service:

```rust
use std::time::Duration;
use tower::Layer;
use tower_rate_limiter::{IpKeyExtractor, MemoryStore, RateLimitLayer};

let limiter = RateLimitLayer::builder(IpKeyExtractor::new())
    .policy_name("public-api")
    .limit(100)
    .window(Duration::from_secs(60))
    .with_store(MemoryStore::new())
    .build()
    .expect("valid rate-limit policy");

let service = limiter.layer(inner_service);
```

This policy allows 100 requests per peer IP in each 60-second fixed window. The next request
receives `429 Too Many Requests` with `Retry-After`. See the
[quick-start guide](https://nivek-ph.github.io/tower-rate-limiter/getting-started.html) for a
complete runnable example.

> `MemoryStore` keeps counters in one process. Use Redis or a custom shared Store when several
> application instances must enforce the same quota.

`IpKeyExtractor` reads the peer socket address from request extensions. When using forwarded
client-IP headers, define and enforce the trusted-proxy boundary first. See
[Axum and Redis](https://nivek-ph.github.io/tower-rate-limiter/adapters.html) and the
[production guide](https://nivek-ph.github.io/tower-rate-limiter/production.html).

## Documentation

- [Quick start](https://nivek-ph.github.io/tower-rate-limiter/getting-started.html)
- [Configuration](https://nivek-ph.github.io/tower-rate-limiter/configuration.html)
- [How it works](https://nivek-ph.github.io/tower-rate-limiter/concepts.html)
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


## Axum integration bounds

When wiring a custom store into Axum helpers, keep `Send + Sync + 'static` (and
`S::Future: Send + 'static`) on the **integration** function, not on the core
`Store` trait:

```rust
fn add_rate_limit<S>(router: axum::Router, store: S) -> Result<axum::Router, ConfigError>
where
    S: Store + Send + Sync + 'static,
    S::Future: Send + 'static,
{
    let layer = RateLimitLayer::builder(IpKeyExtractor::new())
        .with_store(store)
        .build()?;
    Ok(router.layer(layer))
}
```

Axum requires these bounds so the service can move across tasks; the core trait stays
usable in single-threaded contexts without them.
