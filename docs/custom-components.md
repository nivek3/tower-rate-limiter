# Custom components

The core has four application-facing seams. Implement only the ones whose behavior belongs to your
application; `FixedWindow`, the default response factory, and the included fixed-window stores cover
common cases.

## Custom client identity

`KeyExtractor` is synchronous and body-generic:

```rust,ignore
use http::Request;
use tower_rate_limiter::{KeyExtractor, RateLimitError};

#[derive(Clone)]
struct AuthenticatedAccount {
    id: String,
}

#[derive(Clone, Copy)]
struct AccountKey;

impl KeyExtractor for AccountKey {
    type Key = String;

    fn extract<B>(&self, request: &Request<B>) -> Result<Self::Key, RateLimitError> {
        request
            .extensions()
            .get::<AuthenticatedAccount>()
            .map(|account| account.id.clone())
            .ok_or_else(|| RateLimitError::Key(
                "account_missing".into(),
                "authenticated account extension is missing".into(),
            ))
    }
}
```

The key only needs to implement `Display`. Prefer a stable, non-secret identifier. Authentication
and credential validation should happen in an earlier application layer. Normalize identity once
at this boundary rather than composing several extractors.

## Custom policy

`RateLimitPolicy` owns one rate-limit algorithm and quota. It receives a complete policy-scoped key
and returns an asynchronous `Decision`:

```rust,ignore
use std::future::{Ready, ready};
use std::time::Duration;
use tower_rate_limiter::{Decision, RateLimitError, RateLimitPolicy};

#[derive(Clone, Copy)]
struct AlwaysAllow;

impl RateLimitPolicy for AlwaysAllow {
    type Future = Ready<Result<Decision, RateLimitError>>;

    fn check(&self, _key: String) -> Self::Future {
        ready(Ok(Decision::allowed(100, Duration::from_secs(60), 99, Duration::from_secs(60))))
    }
}
```

Return a normal `Decision::rate_limited(...)` when the quota is exhausted. Reserve
`RateLimitError::Policy(code, message)` for a backend or algorithm failure that prevents a
trustworthy Decision. Such errors follow the configured `PolicyFailureMode`.

## Custom fixed-window storage

`FixedWindowStore` is the storage seam used by `FixedWindow`. A store must atomically increment the
complete opaque key and preserve fixed-window semantics:

```rust,ignore
use std::{future::Future, time::Duration};
use tower_rate_limiter::{FixedWindowStore, FixedWindowUsage, RateLimitError};

trait StoreShape: Clone {
    type Future: Future<Output = Result<FixedWindowUsage, RateLimitError>>;
    fn increment(&self, key: &str, window: Duration) -> Self::Future;
}
```

The illustrative `StoreShape` mirrors `tower_rate_limiter::FixedWindowStore`. An implementation
must:

- make increment and first-window creation one atomic operation;
- start expiry only on the first increment;
- avoid extending expiry on later or rejected requests;
- return usage including the current increment;
- return `used >= 1` and the remaining `reset_after` duration;
- make clones of one store value observe the same counters;
- map backend failures to `RateLimitError::Policy(code, message)`.

The store receives a policy-scoped key. It must treat that string as opaque and must not reconstruct
client or policy identity from its format.

`FixedWindowStore` requires `Clone` because `FixedWindow` clones it into each policy check. It
deliberately has no unconditional `Send + Sync + 'static` supertraits, and its Future is not
universally required to be `Send + 'static`. This keeps the core usable by local Tower executors.
Add framework-specific bounds where the policy enters that framework.

When a concrete policy is passed directly to Axum, no extra annotations are normally needed. Rust
derives whether the resulting `ResponseFuture` is `Send + 'static` from its concrete fields and
futures, so built-in policies and custom policies whose futures already satisfy those properties
compile directly:

```rust,ignore
let limiter = RateLimitLayer::builder(IpKeyExtractor::new())
    .with_policy(MyPolicy::new())
    .build()?;

let app = Router::new().layer(limiter);
```

Explicit bounds become necessary when the integration is hidden behind a generic function. Axum
requires the final middleware Future to be `Send + 'static`, but `P: RateLimitPolicy` alone
intentionally does not promise that. State the framework requirement on the generic integration
point:

```rust,ignore
fn add_rate_limit<P>(router: Router, policy: P) -> Result<Router, ConfigError>
where
    P: RateLimitPolicy + Send + Sync + 'static,
    P::Future: Send + 'static,
{
    let limiter = RateLimitLayer::builder(IpKeyExtractor::new())
        .with_policy(policy)
        .build()?;

    Ok(router.layer(limiter))
}
```

The same rule applies when a helper accepts fixed-window storage and constructs `FixedWindow`
itself: state `S: FixedWindowStore + Send + Sync + 'static` and `S::Future: Send + 'static` on the
integration point. The compiler reports the specific future or captured value that prevents the
composed `ResponseFuture` from being `Send`.

## Custom responses

`ResponseFactory` receives the original request and a structured reason. `ReqBody` and `ResBody`
are independent, matching Tower services whose request and response body types differ:

```rust,ignore
use http::{Request, Response};
use tower_rate_limiter::{ResponseFactory, ResponseReason};

#[derive(Clone, Copy)]
struct ApiResponseFactory;

impl<ReqBody, ResBody: Default> ResponseFactory<ReqBody, ResBody> for ApiResponseFactory {
    fn build(&self, _request: Request<ReqBody>, reason: ResponseReason) -> Response<ResBody> {
        let mut response = Response::new(ResBody::default());
        *response.status_mut() = reason.status_code();
        response
    }
}
```

Match `ResponseReason::RateLimited(policy)` to describe quota exhaustion. `policy` carries the
stable policy name and Decision. Match `ResponseReason::Error(...)` to map client-key and policy
failures. The middleware adds `RateLimit`, `RateLimit-Policy`, and `Retry-After` after the factory
returns where applicable.

Avoid placing secrets, raw credentials, or connection details in stable error codes, diagnostic
messages, response bodies, or logs.
