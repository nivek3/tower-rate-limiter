# GCRA backends

GCRA (the generic cell rate algorithm) gives each client key a continuously replenished request
quota. It differs from a fixed window: capacity returns one request at a time rather than all at a
window boundary. `FixedWindow` remains available when that simpler boundary-based behavior is the
right fit.

The crate includes three GCRA policies:

| Policy | State location | Shared across replicas | Runtime / service |
| --- | --- | --- | --- |
| `MemoryGcra` | process memory | no | none; governor |
| `RedisGcra` | Redis | yes | Tokio or Smol Redis connection |
| `PostgresGcra` | PostgreSQL | yes | SQLx Tokio `PgPool` and an installed migration |

Choose a backend according to where the authoritative state must live. Do not use `MemoryGcra`
when a limit must be global across independently running replicas. Redis and PostgreSQL make one
backend state transition per charge; their availability and latency are part of the request path.

## Common quota and decision contract

Construct each policy from a `GcraQuota`:

```rust
use std::time::Duration;
use tower_rate_limiter::GcraQuota;

let quota = GcraQuota::new(100, Duration::from_secs(60), 20)?;
# Ok::<(), tower_rate_limiter::GcraQuotaError>(())
```

- `rate` requests replenish over `period`.
- `burst` is the **total** immediate capacity of a fresh key. It is not capacity added on top of
  `rate`.
- Each included adapter charges exactly one request. Weighted charges and refunds are not part of
  this API.
- The configured emission interval is rounded **up** to whole microseconds. This is the common
  precision of the remotely stored adapters and ensures no included adapter admits faster than the
  configured long-term rate.
- A complete burst must replenish within one year. The constructor rejects longer windows.

All adapters produce `RateLimitPolicy` `Decision` values with the same meanings: an allowed result
contains the configured limit, advertised window, remaining capacity, and reset duration; a
rejection has zero remaining capacity plus an earliest `retry_after`. These values drive the
middleware's `RateLimit` and `Retry-After` fields.

The contract is semantic, not a promise of identical duration values at every instant. Memory uses
its process clock, Redis uses `TIME`, and PostgreSQL captures `clock_timestamp()` after acquiring a
row lock. Network latency, elapsed time while producing a response, and backend-specific rounding
can make individual `reset_after` or `retry_after` values differ slightly. Treat fields as pacing
guidance, not a distributed-clock comparison API.

## Identity, namespace, and versioning

The layer scopes the extracted client key with `policy_name` before calling the policy. Give every
semantically distinct quota a stable policy name. `RedisGcra` and `PostgresGcra` additionally offer
`.with_namespace(...)` to separate applications or deployments that share the same backend.

All replicas that charge the same backend state identity must use the same effective quota. A quota
change changes the interpretation of stored theoretical-arrival-time state. During a rollout,
version the `policy_name` or backend namespace (for example, `public-api-v2` or `service-v2`) so
old and new instances cannot reinterpret the same stored state with different emission intervals.
Allow the old state to expire before retiring its namespace.

## Backend guides

- [Memory GCRA](memory-gcra.md) covers local governor state and its process boundary.
- [Redis GCRA](redis-gcra.md) covers the pinned upstream Lua artifact, Redis ACLs, and topology
  limits.
- [PostgreSQL GCRA](postgres-gcra.md) covers applying the migration, permissions, and expired-state
  cleanup.

## Failure behavior

`MemoryGcra` normally has no remote dependency. Redis and PostgreSQL errors are policy failures:
`PolicyFailureMode::Reject` is the default and returns the configured middleware rejection
(normally `503`); `PolicyFailureMode::Allow` must be selected explicitly and passes the request to
the inner service without rate-limit fields or context. In either mode, configure dependency
timeouts and monitor backend latency and failures.
