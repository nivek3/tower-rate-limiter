//! Tower Service implementation.

use std::{
    sync::Arc,
    task::{Context, Poll},
};

use http::{Request, Response};
use tower_service::Service;

use super::{
    builder::{RateLimitConfig, check_skip_predicate},
    future::ResponseFuture,
    key_extractor::KeyExtractor,
    policy::{RateLimitPolicy, make_key},
    response::ResponseFactory,
};

/// Tower Service produced by [`super::RateLimitLayer`].
#[must_use]
#[derive(Clone, Debug)]
pub struct RateLimit<Inner, K, P, F> {
    pub(crate) inner: Inner,
    pub(crate) key_extractor: K,
    pub(crate) policy: P,
    pub(crate) response_factory: F,
    pub(crate) config: Arc<RateLimitConfig>,
}

impl<Inner, K, P, F> RateLimit<Inner, K, P, F> {
    /// Borrow the wrapped Service.
    pub const fn get_ref(&self) -> &Inner {
        &self.inner
    }

    /// Mutably borrow the wrapped Service.
    pub fn get_mut(&mut self) -> &mut Inner {
        &mut self.inner
    }

    /// Consume this middleware and return the wrapped Service.
    pub fn into_inner(self) -> Inner {
        self.inner
    }
}

impl<Inner, K, P, F, ReqBody, ResBody> Service<Request<ReqBody>> for RateLimit<Inner, K, P, F>
where
    Inner: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone,
    K: KeyExtractor,
    P: RateLimitPolicy,
    F: ResponseFactory<ReqBody, ResBody>,
{
    type Response = Inner::Response;
    type Error = Inner::Error;
    type Future = ResponseFuture<ReqBody, Inner, P, F>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let replacement = self.inner.clone();
        let inner = std::mem::replace(&mut self.inner, replacement);

        let request = match check_skip_predicate(self.config.skip_predicate.as_ref(), request) {
            (true, request) => {
                return ResponseFuture::skipped(
                    request,
                    inner,
                    Arc::clone(&self.config),
                    self.response_factory.clone(),
                );
            },
            (false, request) => request,
        };

        let client_key = match self.key_extractor.extract(&request) {
            Ok(key) => key,
            Err(error) => {
                return ResponseFuture::error(
                    request,
                    error,
                    inner,
                    Arc::clone(&self.config),
                    self.response_factory.clone(),
                );
            },
        };

        let mut key = make_key(&self.config.policy_name, &client_key.to_string());
        if let Some(encoder) = self.config.key_encoder.as_ref() {
            key = encoder(&key);
        }
        let future = self.policy.check(key);

        ResponseFuture::new(
            request,
            inner,
            future,
            Arc::clone(&self.config),
            self.response_factory.clone(),
        )
    }
}
