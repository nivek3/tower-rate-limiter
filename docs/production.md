# Production guide

A rate limiter sits on a trust and availability boundary. Before deploying, make identity, counter
sharing, failure behavior, and rollout compatibility explicit.

## Deployment checklist

- Give every semantically distinct policy a stable, non-empty name.
- Extract identity from validated application state or a verified peer address.
- Define the trusted-proxy boundary before reading forwarding headers.
- Use a policy with shared state when the quota must apply across replicas.
- Decide whether a policy failure rejects or fails open for each policy.
- Apply timeouts and health monitoring to externally backed policies.
- Verify emitted fields and `429` behavior from outside the service.
- Avoid secrets in keys, error messages, response bodies, and logs.

## Identity behind proxies

`IpKeyExtractor` uses only the socket peer supplied in request extensions. It is the safe default
when no trusted-proxy contract exists. `ClientIpKeyExtractor` checks `Forwarded`,
`X-Forwarded-For`, `X-Real-IP`, and `CF-Connecting-IP` before falling back to that socket peer. This
mirrors common reverse-proxy deployments, but the parsed Header value remains an unauthenticated
assertion.

Do not expose `ClientIpKeyExtractor` directly to requests that may supply those headers. The deployment
must define which proxies are trusted and ensure they remove or overwrite every accepted
client-supplied field.

When both trusted proxies and direct clients may reach the listener, use
`TrustedProxyClientIpKeyExtractor` with an application-owned peer policy. It rejects requests without
a socket peer, ignores all forwarding Headers from untrusted peers, and only parses them after the
peer is trusted. The policy can capture application configuration such as exact proxy IPs or a CIDR
set, but the crate never reads environment variables or supplies a network policy. Trusted proxy
ingress must still be protected from direct-client bypass. If that guarantee cannot be made, use an
authenticated identity or `IpKeyExtractor`.

Choose one extractor for the listener and normalize identity once. Chaining socket and
header-aware extractors creates multiple trust decisions for the same request and is outside the
middleware contract.

## Multiple replicas

`MemoryStore` and `MemoryGcra` are process-local. If three replicas each use a 100-request limit,
a caller routed across all three may receive roughly 300 requests across their independent state.
Use `FixedWindow` with `RedisStore`, `RedisGcra`, or `PostgresGcra` when the limit must be global
across replicas.

Make load-balancer behavior part of the decision. Sticky routing may reduce the difference but does
not make process-local state durable or authoritative.

## GCRA backends

All three GCRA adapters share `GcraQuota` and `Decision` semantics, but their clocks and storage
engines differ. `GcraQuota` rounds its emission interval up to whole microseconds. Version a policy
name or backend namespace when rolling out a quota change so different emission intervals never
reinterpret one stored theoretical-arrival-time value. See [GCRA backends](gcra.md) for the shared
contract.

`MemoryGcra` has no remote dependency, but keyed state is local to one process and may grow with
client-key cardinality. Use a bounded application-controlled identity where possible and set a
memory lifecycle budget. Schedule `prune_inactive_keys()` and monitor `tracked_key_count()` when
the key space is not statically bounded.

### Redis

`RedisGcra` shares a continuously replenished quota through ordinary Redis; it does not require a
Redis module. Its Lua charge uses Redis `TIME`, so decisions use the Redis server clock rather than
application-host clocks. Grant its ACL principal `EVALSHA`, `SCRIPT LOAD`, `TIME`, `GET`, and `SET`.

Choose `PolicyFailureMode::Reject` when over-admission is unsafe; it is the default and turns a
Redis failure into the configured middleware response. `PolicyFailureMode::Allow` favors
availability by passing the request through without quota metadata after a policy failure. Neither
choice repairs a lost Redis connection or replaces application timeouts and monitoring.

The Lua artifact uses Redis Lua numeric precision with a known timestamp precision boundary around
2048-09-09. Redis asynchronous replication can also lose recently accepted limiter state during a
failover, briefly allowing more requests. The first release does not promise Redis Cluster,
Sentinel, or managed-proxy compatibility; validate those topologies before relying on the policy.
The artifact is a verbatim copy from `go-redis/redis_rate` v10.0.1; its provenance and license
notices are in [`THIRD_PARTY_NOTICES.md`](https://github.com/nivek-ph/tower-rate-limiter/blob/main/THIRD_PARTY_NOTICES.md).

### PostgreSQL

`PostgresGcra` requires an application-owned SQLx Tokio pool and the migration at
`migrations/postgres/0001_gcra.sql`, applied before serving traffic. The rate-limit principal needs
schema usage, function execution, and the migration's `SELECT`, `INSERT`, and `UPDATE` table
permissions. Schedule bounded deletion of expired rows with a separately privileged maintenance
job when appropriate; PostgreSQL does not clean that state automatically.

The function captures database time after locking the client-key row. A hot key therefore
serializes at that row, and every check consumes PostgreSQL capacity. Monitor lock waits, database
latency, table/index growth, and policy failures. See [PostgreSQL GCRA](postgres-gcra.md) for the
full deployment and permission contract.

## Timeouts and failure policy

The limiter awaits the selected policy future. Bound remote work with the application's timeout
strategy; an unbounded dependency call can hold the request even when the inner service was ready.

Monitor at least:

- rate-limited responses by policy;
- client-key and policy failures by stable error code;
- fail-open events when `PolicyFailureMode::Allow` is used;
- latency of remote policy operations;
- Redis or PostgreSQL connectivity and command failures.

Fail-open responses intentionally contain no quota metadata. Record this path in application
observability without exposing raw keys or credentials. Enabling the `tracing` Cargo feature emits
one structured event for every policy failure on both fail-open and reject paths. Events default to
`WARN`, and each policy may configure another level through the builder. Applications can use the
stable target and fields for filtering, counting, and alerting.

## Policy changes and rollout

Changing a limit keeps the same active counter identity. Changing the policy name or key encoder
creates a different identity, so new requests will not see the old counter.

During a rolling deployment, replicas with mismatched configuration may emit different fields or
charge different counters. Coordinate changes to:

- policy names;
- window durations;
- key extraction rules;
- key encoding;
- backend namespaces;
- rate-limit field revision.

If a policy's window changes, use a new policy name to prevent one logical counter from being used
with incompatible window assumptions.

## Smoke test

For a test policy with limit `2`, send three requests using the same client identity:

```text
request 1 -> inner response; remaining 1
request 2 -> inner response; remaining 0
request 3 -> 429; Retry-After present
```

Then repeat with a different identity and confirm it begins at its own quota. In a replicated
deployment, alternate requests across replicas to verify they share one policy's counter. Finally,
exercise the chosen policy failure mode in a controlled environment.

## Intentional scope

Version 0.2 provides fixed-window request counting and static, one-cost Memory, Redis, and
PostgreSQL GCRA. It does not provide sliding windows, token buckets, weighted requests, refunds,
Redis Cluster lifecycle management, PostgreSQL migration/cleanup automation, or a built-in
proxy-address configuration source.
