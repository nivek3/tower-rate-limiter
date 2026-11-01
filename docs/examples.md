# Examples

The examples cover a framework-independent Tower service, fixed-window counters, and GCRA policies. Every
page below embeds the complete source file used by the repository, so the documentation and the
compiled example cannot drift independently.

| Example | What it demonstrates | Required features |
| --- | --- | --- |
| [Tower with MemoryStore](examples/tower-memory.md) | Minimal Tower Layer composition | `memory` |
| [Axum with nested policies](examples/axum-memory.md) | `ConnectInfo`, global and route-scoped policies | `axum,memory` |
| [Trusted proxy client IP](examples/trusted-proxy-client-ip.md) | Peer-policy-gated client-IP extraction behind a trusted proxy | `axum,memory` |
| [Axum with Redis](examples/axum-redis.md) | Shared fixed-window state, namespace, custom error responses | `axum,redis,runtime-tokio` |
| [Axum with Memory GCRA](examples/axum-memory-gcra.md) | Process-local burst quota and application-owned periodic cleanup | `axum,memory-gcra` |
| [Axum with Redis GCRA](examples/axum-redis-gcra.md) | Shared burst quota through ordinary Redis | `axum,redis-gcra,runtime-tokio` |

Run an example from the repository root with the command shown on its page. The Axum examples start
an HTTP server and keep running until interrupted. The Redis examples additionally require
`REDIS_URL` to reference a reachable Redis server.

These programs are intentionally small. They demonstrate the limiter boundary, not production
authentication, proxy configuration, connection supervision, observability, or graceful shutdown.
Review the [Production guide](production.md) before adapting them to a deployed service.
