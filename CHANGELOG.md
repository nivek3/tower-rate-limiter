# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.1.7](https://github.com/nivek-ph/tower-rate-limiter/compare/v0.1.6...v0.1.7) - 2026-08-20

### Added

- add Redis path HTTP benchmark harness ([#30](https://github.com/nivek-ph/tower-rate-limiter/pull/30))

### Other

- *(deps)* bump redis in the cargo-dependencies group ([#32](https://github.com/nivek-ph/tower-rate-limiter/pull/32))
- *(deps)* bump the cargo-dependencies group across 1 directory with 2 updates ([#28](https://github.com/nivek-ph/tower-rate-limiter/pull/28))
- update README for improved navigation

## [0.1.6](https://github.com/nivek-ph/tower-rate-limiter/compare/v0.1.5...v0.1.6) - 2026-08-13

### Added

- add trusted-proxy client IP extraction and release gates ([#22](https://github.com/nivek-ph/tower-rate-limiter/pull/22))

### Other

- align tower-rate-limiter with Tower conventions ([#24](https://github.com/nivek-ph/tower-rate-limiter/pull/24))

### Added

- Add `TrustedProxyClientIpKeyExtractor` with an application-supplied synchronous trusted-peer
  policy before forwarding Headers are considered.
- Add `RateLimit::{get_ref,get_mut,into_inner}` and public `ResponseFuture` access from the crate root.

### Changed

- Make the crate root the only public module facade; implementation modules are now private.
- Rename `RateLimitService` to `RateLimit` and `RateLimitFuture` to `ResponseFuture`.
- Replace `RateLimitPolicy` with `Policy`: rename `policy_name` to `name`, add the configured
  `window`, and replace the stored `remaining` field with `Policy::remaining()`.
- Change `ResponseReason::RateLimited(limit, usage)` to `ResponseReason::RateLimited(policy)`.
- Narrow `KeyExtractor`, `LimitProvider`, `Store`, and `ResponseFactory` to their semantic bounds so
  callers can use non-`Send` local services and futures where Tower permits them.
- Separate `ReqBody` and `ResBody` throughout the Service/Future path, changing
  `ResponseFactory<B>` to `ResponseFactory<ReqBody, ResBody>`.
- Keep Axum integration focused on request extensions without adding a direct Axum dependency.
- Depend on `tower-layer` and `tower-service` directly; keep the full `tower` utility crate for
  development targets only.
- Return Redis increment futures directly instead of exposing `RedisStoreFuture`.

### Release policy

- On the current 0.x line, automatic release-plz releases remain patch releases. Minor and major
  version changes are prepared manually by the maintainer.

## [0.1.5](https://github.com/nivek-ph/tower-rate-limiter/compare/v0.1.4...v0.1.5) - 2026-08-11

### Fixed

- *(deps)* require http-extract 0.1.3 ([#20](https://github.com/nivek-ph/tower-rate-limiter/pull/20))

## [0.1.4](https://github.com/nivek-ph/tower-rate-limiter/compare/v0.1.3...v0.1.4) - 2026-08-10

### Added

- add trusted-proxy client IP extraction ([#18](https://github.com/nivek-ph/tower-rate-limiter/pull/18))

### Added

- Add `ClientIpKeyExtractor` for trusted-proxy deployments, using strict client-IP Header parsing
  with socket-address fallback.

## [0.1.3](https://github.com/nivek-ph/tower-rate-limiter/compare/v0.1.2...v0.1.3) - 2026-08-10

### Added

- *(tracing)* add optional structured tracing for Store failures ([#16](https://github.com/nivek-ph/tower-rate-limiter/pull/16))

### Other

- update README for clarity and quick start guide
- *(redis)* share RedisStore contract tests across runtimes ([#15](https://github.com/nivek-ph/tower-rate-limiter/pull/15))
- *(redis)* verify RedisStore on the Smol runtime ([#12](https://github.com/nivek-ph/tower-rate-limiter/pull/12))
- *(redis)* add smol support for RedisStore and implement related tests ([#13](https://github.com/nivek-ph/tower-rate-limiter/pull/13))

## [0.1.2](https://github.com/nivek-ph/tower-rate-limiter/compare/v0.1.1...v0.1.2) - 2026-08-09

### Added

- *(redis)* update dependencies and enhance Redis integration ([#11](https://github.com/nivek-ph/tower-rate-limiter/pull/11))
- integrate moka for memory store caching in rate limiter ([#10](https://github.com/nivek-ph/tower-rate-limiter/pull/10))

### Other

- simplify rate limiter internals and tests ([#9](https://github.com/nivek-ph/tower-rate-limiter/pull/9))
- expand mdBook guides and examples ([#6](https://github.com/nivek-ph/tower-rate-limiter/pull/6))
- add mdBook website with GitHub Pages deployment ([#4](https://github.com/nivek-ph/tower-rate-limiter/pull/4))

## [0.1.1](https://github.com/nivek-ph/tower-rate-limiter/compare/v0.1.0...v0.1.1) - 2026-08-07

### Added

- unify rate-limit responses and configuration ([#2](https://github.com/nivek-ph/tower-rate-limiter/pull/2))

### Added

- Add request bypass predicates through `RateLimitBuilder::skip`.
- Add selectable Draft 7 and Draft 11 RateLimit fields, with an option to disable both fields.

### Changed

- Centralize middleware-generated response construction and RateLimit field finalization.
- Rename `StoreErrorAction` to `StoreFailureMode`.
- Rename `RateLimitError` variants to `Key`, `Quota`, and `Store`.
- Rename caller-supplied key transformation APIs to `with_key_encoder` and `KeyEncoder`.

## [0.1.0] - 2026-08-06

### Added

- Add caller-supplied encoding for complete scoped Store keys.
- Add live Redis fixed-window integration testing in CI.

### Changed

- Consolidate charged-request evaluation and response metadata handling.
- Refactor `RateLimitBuilder` configuration while preserving static dispatch.
- Harden Redis fixed-window key handling and script-result validation.
- Pin the minimum supported Rust version to 1.96.0.

## [0.1.0-alpha.0]

### Added

- Initial prerelease.
