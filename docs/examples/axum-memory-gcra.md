# Axum with Memory GCRA

This server allows a fresh peer IP to send three requests immediately, then replenishes one request
every six seconds (10 requests per minute). Additional requests receive `429` with `Retry-After`.

```sh
cargo run --example axum_memory_gcra --no-default-features --features axum,memory-gcra
```

The server listens on `http://127.0.0.1:3000/`. For a fresh key, four immediate requests demonstrate
the burst and rejection:

```sh
for attempt in 1 2 3 4; do curl -i http://127.0.0.1:3000/; done
```

The application schedules `prune_inactive_keys()` every 60 seconds on a clone of the shared policy.
The library starts no cleanup task. Pruning removes inactive keys and does not impose a hard memory
limit; choose the interval and key-space budget for your workload. State is local to this process.

## Complete source

```rust,ignore
{{#include ../../examples/axum_memory_gcra.rs}}
```
