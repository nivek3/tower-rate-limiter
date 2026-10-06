# Rate limit fields

Allowed and rate-limited responses can advertise quota state using `RateLimit-Policy` and
`RateLimit`. Rate-limited responses also include `Retry-After` even when the other fields are
disabled.

Durations are rounded up to whole seconds. Remaining quota uses saturating subtraction, so it stays
at zero after a caller exceeds the limit.

## Draft 11 (default)

For a policy named `public-api`, a quota of 100, and a 60-second window:

```http
RateLimit-Policy: "public-api";q=100;w=60
RateLimit: "public-api";r=99;t=60
```

- `q` is the configured request quota.
- `w` is the policy's advertised window duration.
- `r` is the quota remaining after the current request.
- `t` is the policy-reported time until reset.

The optional quota-unit (`qu`) and partition-key (`pk`) parameters are not emitted. Omitting `qu`
means the quota unit is requests.

## Draft 7

Select the older representation explicitly:

```rust,ignore
use tower_rate_limiter::RateLimitFields;

# let builder = tower_rate_limiter::RateLimitLayer::builder(tower_rate_limiter::IpKeyExtractor::new());
let builder = builder.rate_limit_fields(RateLimitFields::Draft7);
```

It produces fields shaped like:

```http
RateLimit-Policy: 100;w=60
RateLimit: limit=100, remaining=99, reset=60
```

Draft 7 does not carry the configured policy name in either field.

## Disabled

Use `RateLimitFields::Disabled` when another gateway owns these headers or clients must not receive
quota metadata. A rejected request still receives:

```http
Retry-After: 42
```

`Retry-After` reflects the policy's earliest retry duration rounded up to seconds.

## Nested policies

Nested Layers append their values rather than replacing an existing field. Clients should parse
the fields as structured lists and must not assume there is exactly one policy.

The fields are a client-facing projection of current state, not a substitute for server-side
enforcement. A custom `ResponseFactory` controls the response body and status, while the middleware
adds the configured rate-limit fields afterward.
