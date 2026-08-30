# Memory GCRA

`MemoryGcra` is a process-local GCRA `RateLimitPolicy` backed by
[governor](https://crates.io/crates/governor). It is appropriate for one process, development, or
limits intentionally scoped to an individual replica. It needs no async runtime and no external
service.

## Setup

```toml
[dependencies]
tower-rate-limiter = { version = "0.2", default-features = false, features = ["memory-gcra"] }
```

```rust
use std::time::Duration;
use tower_rate_limiter::{GcraQuota, MemoryGcra};

let quota = GcraQuota::new(100, Duration::from_secs(60), 20)?;
let policy = MemoryGcra::new(quota);
# Ok::<(), tower_rate_limiter::GcraQuotaError>(())
```

See [GCRA backends](gcra.md) for burst, microsecond rounding, response-field, and rollout
semantics shared by all three adapters.

## State ownership

Cloning a `MemoryGcra` shares the same keyed governor limiter. Constructing a new `MemoryGcra`
creates a separate state set, even when the quota is identical. State is lost at process restart and
is not shared through a load balancer: three replicas can collectively admit approximately three
times a one-replica limit when requests are distributed between them.

Use `RedisGcra` or `PostgresGcra` when the quota must be authoritative across replicas. Memory
GCRA is not a replacement for a shared quota store.

## Key cardinality

The limiter holds keyed state in memory. Choose a bounded, application-controlled client-key space
where possible, and treat unbounded attacker-controlled identities as a capacity risk. Set an
application memory budget and lifecycle strategy before exposing a high-cardinality limiter.

Call `prune_inactive_keys()` periodically to discard keys whose state is already equivalent to a
fresh key. `tracked_key_count()` exposes an approximate count for capacity monitoring:

```rust
# use std::time::Duration;
# use tower_rate_limiter::{GcraQuota, MemoryGcra};
# let quota = GcraQuota::new(100, Duration::from_secs(60), 20)?;
# let policy = MemoryGcra::new(quota);
policy.prune_inactive_keys();
let tracked = policy.tracked_key_count();
# let _ = tracked;
# Ok::<(), tower_rate_limiter::GcraQuotaError>(())
```

The crate does not start a background cleanup task; the application controls scheduling and can
align it with its own runtime and memory budget.

The policy charges before the downstream service runs, so an allowed request still consumes one
cell if its handler later fails. There is no refund API.
