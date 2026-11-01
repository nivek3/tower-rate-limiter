# Axum with Redis GCRA

This server enforces the same quota as the [Memory GCRA example](axum-memory-gcra.md): three
requests immediately per fresh peer IP, then one request replenished every six seconds.

Set `REDIS_URL` to a reachable Redis server with scripting enabled:

```sh
export REDIS_URL=redis://127.0.0.1:6379/
cargo run --example axum_redis_gcra --no-default-features --features axum,redis-gcra,runtime-tokio
```

The server listens on `http://127.0.0.1:3000/`. For a fresh key, the fourth immediate request
receives `429` with `Retry-After`:

```sh
for attempt in 1 2 3 4; do curl -i http://127.0.0.1:3000/; done
```

Redis keeps quota state across application restarts. Replicas using the same namespace, policy
name, client key and quota share that state. The example owns connection creation; see the
[Redis GCRA guide](../redis-gcra.md) for permissions and deployment limits.

## Complete source

```rust,ignore
{{#include ../../examples/axum_redis_gcra.rs}}
```
