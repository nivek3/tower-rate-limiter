//! The request-aware rate-limiting seam.
//!
//! Public interfaces are re-exported here; private files group builder configuration, the Tower
//! lifecycle, Policy ownership, Decision state, errors, and response finalization.

mod builder;
mod error;
mod future;
mod key_extractor;
mod layer;
mod policy;
mod response;
mod service;
mod store;

pub use builder::RateLimitBuilder;
pub(crate) use builder::RateLimitConfig;
pub use error::{ConfigError, RateLimitError};
pub use future::ResponseFuture;
pub use key_extractor::{ClientIpKeyExtractor, IpKeyExtractor, KeyExtractor, TrustedProxyClientIpKeyExtractor};
pub use layer::RateLimitLayer;
pub use policy::{Decision, DecisionOutcome, PolicyDecision, RateLimitContext, RateLimitPolicy};
pub use response::{DefaultResponseFactory, RateLimitFields, ResponseFactory, ResponseReason};
pub use service::RateLimit;
pub use store::{FixedWindow, FixedWindowFuture, FixedWindowStore, FixedWindowUsage, PolicyFailureMode};
