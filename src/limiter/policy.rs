//! Algorithm-independent Policy Decisions and response metadata.

use std::{future::Future, time::Duration};

use http::Request;

use super::{RateLimitError, response::RateLimitFields};

/// The outcome of charging one request against a Policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DecisionOutcome {
    /// The Charged Request may call the inner service.
    Allowed,
    /// The Charged Request exceeded its Quota.
    RateLimited,
}

/// The algorithm-independent result of charging one request against a Policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Decision {
    outcome: DecisionOutcome,
    limit: u64,
    window: Duration,
    remaining: u64,
    reset_after: Duration,
    retry_after: Option<Duration>,
}

impl Decision {
    /// Construct an allowed Decision.
    pub const fn allowed(limit: u64, window: Duration, remaining: u64, reset_after: Duration) -> Self {
        Self {
            outcome: DecisionOutcome::Allowed,
            limit,
            window,
            remaining,
            reset_after,
            retry_after: None,
        }
    }

    /// Construct a rate-limited Decision.
    pub const fn rate_limited(limit: u64, window: Duration, reset_after: Duration, retry_after: Duration) -> Self {
        Self {
            outcome: DecisionOutcome::RateLimited,
            limit,
            window,
            remaining: 0,
            reset_after,
            retry_after: Some(retry_after),
        }
    }

    /// Return the Decision outcome.
    pub const fn outcome(&self) -> DecisionOutcome {
        self.outcome
    }

    /// Return true when the request exceeded its Quota.
    pub const fn is_rate_limited(&self) -> bool {
        matches!(self.outcome, DecisionOutcome::RateLimited)
    }

    /// Return the advertised Quota amount.
    pub const fn limit(&self) -> u64 {
        self.limit
    }

    /// Return the advertised Quota window.
    pub const fn window(&self) -> Duration {
        self.window
    }

    /// Return the remaining instantaneous capacity after the charge.
    pub const fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Return the duration until the Policy returns to full capacity.
    pub const fn reset_after(&self) -> Duration {
        self.reset_after
    }

    /// Return the earliest retry duration for a rejected request.
    pub const fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }
}

/// An asynchronous Policy that owns one Algorithm and Quota.
///
/// The middleware supplies a complete, scoped, and optionally encoded Client Key. Implementations
/// must return a normal rate-limited Decision when Quota is exhausted and reserve errors for cases
/// where a trustworthy Decision could not be produced.
pub trait RateLimitPolicy: Clone {
    /// The concrete future returned by [`RateLimitPolicy::check`].
    type Future: Future<Output = Result<Decision, RateLimitError>>;

    /// Charge one request for `key`.
    fn check(&self, key: String) -> Self::Future;
}

/// A Policy name paired with the Decision produced for one Charged Request.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicyDecision {
    name: String,
    decision: Decision,
}

impl PolicyDecision {
    pub(super) fn new(name: String, decision: Decision) -> Self {
        Self { name, decision }
    }

    /// Return the stable Policy identifier.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the Decision produced for this Policy.
    pub const fn decision(&self) -> &Decision {
        &self.decision
    }
}

/// Read-only rate-limit state carried in request extensions for allowed requests.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RateLimitContext {
    decisions: Vec<PolicyDecision>,
}

impl RateLimitContext {
    /// Construct an empty context.
    pub const fn new() -> Self {
        Self { decisions: Vec::new() }
    }

    /// Borrow all named Policy Decisions in composition order.
    pub fn decisions(&self) -> &[PolicyDecision] {
        &self.decisions
    }
}

/// Internal response-field configuration for one charged Policy.
#[derive(Clone, Debug)]
pub(super) struct ResponseMetadata {
    pub(super) policy_decision: PolicyDecision,
    pub(super) fields: RateLimitFields,
}

impl ResponseMetadata {
    pub(super) fn new(policy_decision: PolicyDecision, fields: RateLimitFields) -> Self {
        Self {
            policy_decision,
            fields,
        }
    }
}

/// Build the scoped Client Key passed to a [`RateLimitPolicy`].
pub(super) fn make_key(policy_name: &str, client_key: &str) -> String {
    format!("{}:{}", escape_key_part(policy_name), escape_key_part(client_key))
}

fn escape_key_part(value: &str) -> String {
    value.replace('%', "%25").replace(':', "%3A")
}

/// Append the public Policy projection to the request extensions.
pub(super) fn append_context<B>(request: &mut Request<B>, metadata: &ResponseMetadata) {
    request
        .extensions_mut()
        .get_or_insert_default::<RateLimitContext>()
        .decisions
        .push(metadata.policy_decision.clone());
}
