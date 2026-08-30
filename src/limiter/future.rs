//! The unboxed request-execution state machine for the rate-limit Service.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};

use http::{Request, Response};
use pin_project_lite::pin_project;

use super::{
    RateLimitConfig, RateLimitError,
    policy::{PolicyDecision, RateLimitPolicy, ResponseMetadata, append_context},
    response::{MiddlewareResponse, ResponseFactory, append_inner_response_headers},
    store::PolicyFailureMode,
};

pin_project! {
    #[project = StateProj]
    #[project_replace = StateProjReplace]
    enum State<ReqBody, PolicyFut, InnerFut> {
        Policy {
            #[pin]
            future: PolicyFut,
            request: Request<ReqBody>,
        },
        Inner {
            #[pin]
            future: InnerFut,
            metadata: Option<ResponseMetadata>,
        },
        Ready {
            response: MiddlewareResponse<ReqBody>,
        },
        Done,
    }
}

pin_project! {
    /// Response future for [`super::RateLimit`].
    pub struct ResponseFuture<ReqBody, Inner, P, F>
        where
        Inner: tower_service::Service<Request<ReqBody>>,
        P: RateLimitPolicy,
    {
        #[pin]
        state: State<ReqBody, P::Future, Inner::Future>,
        inner: Inner,
        config: Arc<RateLimitConfig>,
        factory: F,
    }
}

impl<ReqBody, Inner, P, F> ResponseFuture<ReqBody, Inner, P, F>
where
    Inner: tower_service::Service<Request<ReqBody>>,
    P: RateLimitPolicy,
{
    pub(crate) fn new(
        request: Request<ReqBody>,
        inner: Inner,
        future: P::Future,
        config: Arc<RateLimitConfig>,
        factory: F,
    ) -> Self {
        Self {
            state: State::Policy { request, future },
            inner,
            config,
            factory,
        }
    }

    pub(crate) fn error(
        request: Request<ReqBody>,
        error: RateLimitError,
        inner: Inner,
        config: Arc<RateLimitConfig>,
        factory: F,
    ) -> Self {
        Self {
            state: State::Ready {
                response: MiddlewareResponse::Error(request, error),
            },
            inner,
            config,
            factory,
        }
    }

    pub(crate) fn skipped(
        request: Request<ReqBody>,
        mut inner: Inner,
        config: Arc<RateLimitConfig>,
        factory: F,
    ) -> Self {
        let future = inner.call(request);
        Self {
            state: State::Inner { future, metadata: None },
            inner,
            config,
            factory,
        }
    }
}

impl<ReqBody, ResBody, Inner, P, F> Future for ResponseFuture<ReqBody, Inner, P, F>
where
    Inner: tower_service::Service<Request<ReqBody>, Response = Response<ResBody>>,
    P: RateLimitPolicy,
    F: ResponseFactory<ReqBody, ResBody>,
{
    type Output = Result<Response<ResBody>, Inner::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.state.as_mut().project() {
                StateProj::Policy { future, .. } => {
                    let result = ready!(future.poll(cx));
                    let StateProjReplace::Policy { request, .. } = this.state.as_mut().project_replace(State::Done)
                    else {
                        unreachable!("rate-limit future state changed while polling Policy")
                    };

                    let next_state = match result {
                        Err(error) => {
                            #[cfg(feature = "tracing")]
                            trace_policy_failure(
                                &error,
                                this.config.policy_failure_mode,
                                &this.config.policy_name,
                                this.config.policy_failure_tracing_level,
                            );
                            if this.config.policy_failure_mode == PolicyFailureMode::Allow {
                                State::Inner {
                                    future: this.inner.call(request),
                                    metadata: None,
                                }
                            } else {
                                State::Ready {
                                    response: MiddlewareResponse::Error(request, error),
                                }
                            }
                        },
                        Ok(decision) => {
                            let policy_decision = PolicyDecision::new(this.config.policy_name.clone(), decision);
                            let metadata = ResponseMetadata::new(policy_decision, this.config.rate_limit_fields);
                            if decision.is_rate_limited() {
                                State::Ready {
                                    response: MiddlewareResponse::RateLimited(request, metadata),
                                }
                            } else {
                                let mut request = request;
                                append_context(&mut request, &metadata);
                                State::Inner {
                                    future: this.inner.call(request),
                                    metadata: Some(metadata),
                                }
                            }
                        },
                    };
                    this.state.as_mut().project_replace(next_state);
                },
                StateProj::Inner { future, .. } => {
                    let result = ready!(future.poll(cx));
                    let StateProjReplace::Inner { metadata, .. } = this.state.as_mut().project_replace(State::Done)
                    else {
                        unreachable!("rate-limit future state changed while polling inner Service")
                    };
                    return Poll::Ready(result.map(|response| append_inner_response_headers(response, metadata)));
                },
                StateProj::Ready { .. } => {
                    let StateProjReplace::Ready { response } = this.state.as_mut().project_replace(State::Done) else {
                        unreachable!("rate-limit future state changed while returning response")
                    };
                    return Poll::Ready(Ok(response.finalize(this.factory)));
                },
                StateProj::Done { .. } => panic!("rate-limit response future polled after completion"),
            }
        }
    }
}

#[cfg(feature = "tracing")]
fn trace_policy_failure(
    error: &RateLimitError,
    failure_mode: PolicyFailureMode,
    policy_name: &str,
    level: tracing::Level,
) {
    let failure_mode = match failure_mode {
        PolicyFailureMode::Reject => "reject",
        PolicyFailureMode::Allow => "allow",
    };

    macro_rules! emit {
        ($level:expr) => {
            tracing::event!(
                target: "tower_rate_limiter::policy",
                $level,
                event = "policy_failure",
                policy_name,
                failure_mode,
                error_code = error.code(),
                "rate-limit Policy failed"
            )
        };
    }

    match level {
        tracing::Level::ERROR => emit!(tracing::Level::ERROR),
        tracing::Level::WARN => emit!(tracing::Level::WARN),
        tracing::Level::INFO => emit!(tracing::Level::INFO),
        tracing::Level::DEBUG => emit!(tracing::Level::DEBUG),
        tracing::Level::TRACE => emit!(tracing::Level::TRACE),
    }
}
