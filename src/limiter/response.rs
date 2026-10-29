//! Middleware response construction and Rate Limit Fields.
//!
//! Owns the public response customization seam and the private finalization
//! path shared by every response produced before the inner service is called.

use std::time::Duration;

use http::{HeaderValue, Request, Response, StatusCode, header::HeaderName};

use super::{
    error::RateLimitError,
    policy::{PolicyDecision, ResponseMetadata},
};

/// The `RateLimit` header name.
const RATE_LIMIT: HeaderName = HeaderName::from_static("ratelimit");
/// The `RateLimit-Policy` header name.
const RATE_LIMIT_POLICY: HeaderName = HeaderName::from_static("ratelimit-policy");
/// The `Retry-After` header name.
const RETRY_AFTER: HeaderName = HeaderName::from_static("retry-after");

/// The Rate Limit Fields revision emitted in responses.
///
/// Draft 7 represents `RateLimit` as a dictionary containing `limit`, `remaining`, and `reset`.
/// Its `RateLimit-Policy` field contains the quota and window without a policy identifier.
///
/// Draft 11 represents both fields as lists of named items. This crate emits its fixed-window
/// `q`/`w` and `r`/`t` parameters; optional quota-unit (`qu`) and partition-key (`pk`) parameters
/// are not emitted.
///
/// See the field definitions for [draft 7] and [draft 11].
///
/// [draft 7]: https://datatracker.ietf.org/doc/html/draft-ietf-httpapi-ratelimit-headers-07#name-ratelimit-header-field-def
/// [draft 11]: https://datatracker.ietf.org/doc/html/draft-ietf-httpapi-ratelimit-headers-11#name-ratelimit-policy-field
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum RateLimitFields {
    /// Emit fields compatible with draft 7.
    Draft7,
    /// Emit fields compatible with draft 11.
    #[default]
    Draft11,
    /// Do not emit `RateLimit` or `RateLimit-Policy` fields.
    /// Rate-limited responses still include `Retry-After`.
    Disabled,
}

/// The structured reason passed to a [`ResponseFactory`].
#[derive(Debug)]
pub enum ResponseReason {
    /// The named Policy Decision that rejected the Charged Request.
    RateLimited(PolicyDecision),
    /// The middleware could not resolve or charge the request's Policy.
    Error(RateLimitError),
}

impl ResponseReason {
    /// Return the default HTTP status for this reason.
    pub const fn status_code(&self) -> StatusCode {
        match self {
            Self::RateLimited(_) => StatusCode::TOO_MANY_REQUESTS,
            Self::Error(RateLimitError::Key(_, _)) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Error(RateLimitError::Policy(_, _)) => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

/// A factory for responses to rate-limit middleware results.
pub trait ResponseFactory<ReqBody, ResBody>: Clone {
    /// Build a response using the original request and structured reason.
    fn build(&self, request: Request<ReqBody>, reason: ResponseReason) -> Response<ResBody>;
}

/// The default empty-body response factory.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct DefaultResponseFactory;

impl<ReqBody, ResBody> ResponseFactory<ReqBody, ResBody> for DefaultResponseFactory
where
    ResBody: Default,
{
    fn build(&self, _request: Request<ReqBody>, reason: ResponseReason) -> Response<ResBody> {
        let mut response = Response::new(ResBody::default());
        *response.status_mut() = reason.status_code();
        response
    }
}

/// A response produced by the middleware before the inner service is called.
///
/// Response construction and rate-limit field decoration stay deferred until
/// the future's common ready state.
pub(super) enum MiddlewareResponse<ReqBody> {
    RateLimited(Request<ReqBody>, ResponseMetadata),
    Error(Request<ReqBody>, RateLimitError),
}

impl<ReqBody> MiddlewareResponse<ReqBody> {
    /// Build and decorate the final HTTP response through one middleware path.
    pub(super) fn finalize<ResBody, Factory>(self, factory: &Factory) -> Response<ResBody>
    where
        Factory: ResponseFactory<ReqBody, ResBody>,
    {
        match self {
            Self::RateLimited(request, metadata) => {
                let reason = ResponseReason::RateLimited(metadata.policy_decision.clone());
                let response = factory.build(request, reason);

                append_rate_limited_response_headers(response, metadata)
            },
            Self::Error(request, error) => factory.build(request, ResponseReason::Error(error)),
        }
    }
}

/// Decorate an allowed (or fail-open) inner response with Rate Limit Fields.
///
/// `None` means no quota metadata is available after bypass or fail-open, so no fields are
/// written. When present, fields follow [`RateLimitFields`]; `Retry-After` is never added on this
/// path.
pub(super) fn append_inner_response_headers<B>(
    response: Response<B>,
    metadata: Option<ResponseMetadata>,
) -> Response<B> {
    match metadata {
        Some(metadata) => append_rate_limit_fields(response, &metadata),
        None => response,
    }
}

/// Decorate a rate-limited middleware response with Rate Limit Fields and Retry-After.
///
/// Rate Limit Fields still respect [`RateLimitFields`]. `Retry-After` is always added.
fn append_rate_limited_response_headers<B>(response: Response<B>, metadata: ResponseMetadata) -> Response<B> {
    let mut response = append_rate_limit_fields(response, &metadata);
    let retry_after = metadata
        .policy_decision
        .decision()
        .retry_after()
        .expect("rate-limited Decision must include retry-after");
    append_header(&mut response, RETRY_AFTER, &ceil_seconds(retry_after).to_string());
    response
}

/// Shared Rate Limit / RateLimit-Policy field writer.
fn append_rate_limit_fields<B>(response: Response<B>, metadata: &ResponseMetadata) -> Response<B> {
    let Some((policy, rate_limit)) = format_rate_limit_fields(metadata) else {
        return response;
    };

    let mut response = response;
    append_header(&mut response, RATE_LIMIT_POLICY, &policy);
    append_header(&mut response, RATE_LIMIT, &rate_limit);
    response
}

/// Format Rate Limit Fields for the configured revision.
fn format_rate_limit_fields(metadata: &ResponseMetadata) -> Option<(String, String)> {
    if metadata.fields == RateLimitFields::Disabled {
        return None;
    }

    let decision = metadata.policy_decision.decision();
    let limit = decision.limit();
    let remaining = decision.remaining();
    let reset_after = ceil_seconds(decision.reset_after());
    let window = ceil_seconds(decision.window());

    Some(match metadata.fields {
        RateLimitFields::Draft7 => (
            format!("{limit};w={window}"),
            format!("limit={limit}, remaining={remaining}, reset={reset_after}"),
        ),
        RateLimitFields::Draft11 => {
            let policy_name = quote_structured_string(metadata.policy_decision.name());

            (
                format!("{policy_name};q={limit};w={window}"),
                format!("{policy_name};r={remaining};t={reset_after}"),
            )
        },
        RateLimitFields::Disabled => return None,
    })
}

/// Quote one validated visible-ASCII value as an HTTP Structured Fields string.
fn quote_structured_string(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for byte in value.bytes() {
        if byte == b'"' || byte == b'\\' {
            quoted.push('\\');
        }
        quoted.push(char::from(byte));
    }
    quoted.push('"');
    quoted
}

/// Append a header to the response.
fn append_header<B>(response: &mut Response<B>, name: HeaderName, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        response.headers_mut().append(name, value);
    }
}

/// Ceil the duration to the nearest second.
fn ceil_seconds(duration: Duration) -> u64 {
    duration
        .as_secs()
        .saturating_add(u64::from(duration.subsec_nanos() != 0))
        .max(1)
}
