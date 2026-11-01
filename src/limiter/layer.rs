//! Tower Layer implementation.

use std::sync::Arc;

use tower_layer::Layer;

use super::{
    builder::RateLimitConfig, key_extractor::KeyExtractor, policy::RateLimitPolicy, response::DefaultResponseFactory,
    service::RateLimit,
};

/// Layer that applies [`RateLimit`].
#[derive(Clone, Debug)]
#[must_use]
pub struct RateLimitLayer<K, P = (), F = DefaultResponseFactory> {
    pub(crate) key_extractor: K,
    pub(crate) policy: P,
    pub(crate) response_factory: F,
    pub(crate) config: Arc<RateLimitConfig>,
}

impl<K, P, F, Inner> Layer<Inner> for RateLimitLayer<K, P, F>
where
    K: KeyExtractor,
    P: RateLimitPolicy,
    F: Clone,
{
    type Service = RateLimit<Inner, K, P, F>;

    fn layer(&self, inner: Inner) -> Self::Service {
        RateLimit {
            inner,
            key_extractor: self.key_extractor.clone(),
            policy: self.policy.clone(),
            response_factory: self.response_factory.clone(),
            config: Arc::clone(&self.config),
        }
    }
}
