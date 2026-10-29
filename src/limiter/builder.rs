//! Builder and immutable configuration for the rate-limit Layer.

use std::{fmt, sync::Arc};

use http::Request;

use super::{
    error::ConfigError,
    layer::RateLimitLayer,
    policy::RateLimitPolicy,
    response::{DefaultResponseFactory, RateLimitFields},
    store::PolicyFailureMode,
};

/// A callback to encode the scoped Client Key before passing it to the Policy.
pub(crate) type KeyEncoder = Box<dyn Fn(&str) -> String + Send + Sync>;

/// A callback that decides whether a request bypasses rate limiting.
pub(crate) type SkipPredicate = Box<dyn Fn(&Request<()>) -> bool + Send + Sync>;

pub(crate) fn check_skip_predicate<B>(predicate: Option<&SkipPredicate>, request: Request<B>) -> (bool, Request<B>) {
    let Some(predicate) = predicate else {
        return (false, request);
    };

    let (parts, body) = request.into_parts();
    let request_head = Request::from_parts(parts, ());
    let should_skip = predicate(&request_head);
    let (parts, ()) = request_head.into_parts();

    (should_skip, Request::from_parts(parts, body))
}

/// Builder for a rate-limit Layer with compile-time Policy and response-factory types.
#[derive(Debug)]
#[must_use]
pub struct RateLimitBuilder<K, P = (), F = DefaultResponseFactory> {
    key_extractor: K,
    policy: P,
    response_factory: F,
    config: RateLimitConfig,
}

impl<K> RateLimitBuilder<K> {
    pub(crate) fn new(key_extractor: K) -> Self {
        Self {
            key_extractor,
            policy: (),
            response_factory: DefaultResponseFactory,
            config: RateLimitConfig {
                policy_name: String::from("default-policy"),
                key_encoder: None,
                skip_predicate: None,
                policy_failure_mode: PolicyFailureMode::default(),
                #[cfg(feature = "tracing")]
                policy_failure_tracing_level: tracing::Level::WARN,
                rate_limit_fields: RateLimitFields::default(),
            },
        }
    }
}

impl<K, P, F> RateLimitBuilder<K, P, F> {
    /// Select the Policy and update the builder's Policy type state.
    pub fn with_policy<P2>(self, policy: P2) -> RateLimitBuilder<K, P2, F> {
        let Self {
            key_extractor,
            response_factory,
            config,
            ..
        } = self;
        RateLimitBuilder {
            key_extractor,
            policy,
            response_factory,
            config,
        }
    }

    /// Replace the response factory and update its type state.
    pub fn response_factory<F2>(self, response_factory: F2) -> RateLimitBuilder<K, P, F2> {
        let Self {
            key_extractor,
            policy,
            config,
            ..
        } = self;
        RateLimitBuilder {
            key_extractor,
            policy,
            response_factory,
            config,
        }
    }

    /// Set the stable Policy identifier used in the scoped Client Key and response metadata.
    pub fn policy_name(mut self, policy_name: impl Into<String>) -> Self {
        self.config.policy_name = policy_name.into();
        self
    }

    /// Encode the scoped Client Key before passing it to the Policy.
    pub fn with_key_encoder<E>(mut self, encoder: E) -> Self
    where
        E: Fn(&str) -> String + Send + Sync + 'static,
    {
        self.config.key_encoder = Some(Box::new(encoder));
        self
    }

    /// Bypass rate limiting when `predicate` returns true for the request head.
    pub fn skip<Predicate>(mut self, predicate: Predicate) -> Self
    where
        Predicate: Fn(&Request<()>) -> bool + Send + Sync + 'static,
    {
        self.config.skip_predicate = Some(Box::new(predicate));
        self
    }

    /// Select the behavior used when the Policy cannot produce a trustworthy Decision.
    pub fn policy_failure_mode(mut self, mode: PolicyFailureMode) -> Self {
        self.config.policy_failure_mode = mode;
        self
    }

    /// Select the tracing level used for Policy failure events.
    #[cfg(feature = "tracing")]
    pub fn policy_failure_tracing_level(mut self, level: tracing::Level) -> Self {
        self.config.policy_failure_tracing_level = level;
        self
    }

    /// Select the Rate Limit Fields revision emitted in responses.
    pub fn rate_limit_fields(mut self, fields: RateLimitFields) -> Self {
        self.config.rate_limit_fields = fields;
        self
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.config.policy_name.is_empty() {
            return Err(ConfigError::EmptyPolicyName);
        }
        if !self
            .config
            .policy_name
            .bytes()
            .all(|byte| (0x20..=0x7e).contains(&byte))
        {
            return Err(ConfigError::InvalidPolicyName);
        }
        Ok(())
    }
}

impl<K> RateLimitLayer<K, (), DefaultResponseFactory> {
    /// Start a typed rate-limit Layer builder.
    pub fn builder(key_extractor: K) -> RateLimitBuilder<K> {
        RateLimitBuilder::new(key_extractor)
    }
}

impl<K, P, F> RateLimitBuilder<K, P, F>
where
    P: RateLimitPolicy,
{
    /// Validate the builder and produce a configured Layer.
    pub fn build(self) -> Result<RateLimitLayer<K, P, F>, ConfigError> {
        self.validate()?;
        Ok(RateLimitLayer {
            key_extractor: self.key_extractor,
            policy: self.policy,
            response_factory: self.response_factory,
            config: Arc::new(self.config),
        })
    }
}

/// Immutable configuration shared by every Service produced from a Layer.
pub(crate) struct RateLimitConfig {
    pub(crate) policy_name: String,
    pub(crate) key_encoder: Option<KeyEncoder>,
    pub(crate) skip_predicate: Option<SkipPredicate>,
    pub(crate) policy_failure_mode: PolicyFailureMode,
    #[cfg(feature = "tracing")]
    pub(crate) policy_failure_tracing_level: tracing::Level,
    pub(crate) rate_limit_fields: RateLimitFields,
}

impl fmt::Debug for RateLimitConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("RateLimitConfig");
        debug
            .field("policy_name", &self.policy_name)
            .field("has_key_encoder", &self.key_encoder.is_some())
            .field("has_skip_predicate", &self.skip_predicate.is_some())
            .field("policy_failure_mode", &self.policy_failure_mode);
        #[cfg(feature = "tracing")]
        debug.field("policy_failure_tracing_level", &self.policy_failure_tracing_level);
        debug.field("rate_limit_fields", &self.rate_limit_fields).finish()
    }
}
