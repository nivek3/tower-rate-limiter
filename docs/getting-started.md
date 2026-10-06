# Quick start

This guide builds a process-local limiter around a plain Tower service. It is the shortest path to
the first working layer; the same layer can later be applied to an Axum router.

## 1. Add the dependency

The default feature enables the in-memory `FixedWindowStore`:

```toml
[dependencies]
tower-rate-limiter = "0.2"
```

The crate requires Rust 1.96 or newer.

## 2. Choose a client key

Define how a request becomes an application-owned client key and compose the Layer:

```rust,ignore
{{#include ../examples/tower_memory.rs}}
```

Run the complete example from the repository root:

```sh
cargo run --example tower_memory --features memory
```

The example uses one static key so its construction is easy to see. In a real service, extract a
validated account ID, API-client ID, peer address, or another stable identity. Different callers
must produce different keys; repeated requests from one caller must produce the same key.

## 3. Configure the policy deliberately

The minimal production-shaped builder is:

```rust,ignore
use std::time::Duration;
use tower_rate_limiter::{FixedWindow, IpKeyExtractor, MemoryStore, RateLimitLayer};

let layer = RateLimitLayer::builder(IpKeyExtractor::new())
    .policy_name("public-api")
    .with_policy(FixedWindow::new(MemoryStore::new(), 100, Duration::from_secs(60))?)
    .build()?;
# Ok::<(), tower_rate_limiter::ConfigError>(())
```

`build()` validates layer configuration, including a non-empty policy name. `FixedWindow::new(...)`
validates its window, which must be at least one millisecond. The typed builder also prevents
`build()` until a policy has been supplied.

## Builder defaults

| Setting | Default |
| --- | --- |
| Policy name | `default-policy` |
| Policy errors | Reject with `503 Service Unavailable` |
| Response fields | IETF draft 11 |

A real application should set a stable policy name and select its policy deliberately. Layers that
share a policy instance or backend state, policy name, and extracted key intentionally share usage.

## What happens on each request

The middleware first extracts a key, scopes it with the policy name, then asks the selected policy
to charge it. A policy failure consumes no quota metadata, while a charged request is not refunded
based on the downstream response.

The first `limit` requests are allowed. Request `limit + 1` is rate limited, and rejected requests
continue to increment usage without extending the active fixed window.

For a limit of `2`, the sequence is:

| Request | `FixedWindowUsage::used` | Remaining | Result |
| --- | ---: | ---: | --- |
| 1 | 1 | 1 | inner service called |
| 2 | 2 | 0 | inner service called |
| 3 | 3 | 0 | `429 Too Many Requests` |

The response from the second request has zero remaining quota but is still allowed. Exhaustion is
enforced only when `used > limit`.

## Next steps

- Read [How it works](concepts.md) for the public extension points.
- Browse the [complete examples](examples.md) for Tower, Axum, proxy handling, and Redis.
- Review every builder option in [Configuration](configuration.md).
- Use [Axum and Redis](adapters.md) when the service needs framework or shared-store integration.
- Browse the repository's [`examples/`](https://github.com/nivek-ph/tower-rate-limiter/tree/main/examples)
  for nested policies, proxy handling, and custom error responses.
