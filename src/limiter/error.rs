//! Error types for the rate limiter.

use std::time::Duration;

/// A failure produced while resolving or charging a rate-limit policy.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum RateLimitError {
    /// The client key could not be resolved.
    #[error("client key unavailable ({0}): {1}")]
    Key(String, String),
    /// The selected policy could not charge the request reliably.
    #[error("rate-limit policy unavailable ({0}): {1}")]
    Policy(String, String),
}

impl RateLimitError {
    /// Return the stable machine-readable error code.
    pub fn code(&self) -> &str {
        match self {
            Self::Key(code, _) | Self::Policy(code, _) => code,
        }
    }
}

/// Errors produced while validating a rate-limit builder.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// The configured window is shorter than the one-millisecond minimum.
    #[error("rate-limit window {0:?} is shorter than the minimum {1:?}")]
    WindowTooShort(Duration, Duration),
    /// The policy identifier is invalid.
    #[error("empty policy name")]
    EmptyPolicyName,
    /// The policy identifier contains bytes that cannot be represented as an HTTP Structured
    /// Fields string.
    #[error("policy name must contain only visible ASCII characters")]
    InvalidPolicyName,
}
