use std::{env, error::Error, net::SocketAddr, time::Duration};

use axum::{Router, routing::get};
use http::{Request, Response, StatusCode};
use tower_rate_limiter::{
    FixedWindow, IpKeyExtractor, KeyExtractor, RateLimitError, RateLimitLayer, RedisStore, ResponseFactory,
    ResponseReason,
};

/// Demo extractor: read a client key from `X-User-Id`.
/// Real apps should resolve identity in an earlier auth layer and read an extension instead.
#[derive(Clone, Copy)]
struct UserIdKeyExtractor;

impl KeyExtractor for UserIdKeyExtractor {
    type Key = String;

    fn extract<B>(&self, request: &Request<B>) -> Result<Self::Key, RateLimitError> {
        let value = request
            .headers()
            .get("x-user-id")
            .ok_or_else(missing_user_id)?
            .to_str()
            .map_err(|_| invalid_user_id())?;

        if value.is_empty() {
            return Err(missing_user_id());
        }

        Ok(value.to_owned())
    }
}

fn missing_user_id() -> RateLimitError {
    RateLimitError::Key(
        String::from("missing_user_id"),
        String::from("x-user-id header is required"),
    )
}

fn invalid_user_id() -> RateLimitError {
    RateLimitError::Key(
        String::from("invalid_user_id"),
        String::from("x-user-id must be valid UTF-8"),
    )
}

/// Example-only HTTP mapping for the application-owned user identity extractor.
#[derive(Clone, Copy, Debug, Default)]
struct AuthResponseFactory;

impl<ReqBody, ResBody> ResponseFactory<ReqBody, ResBody> for AuthResponseFactory
where
    ResBody: Default,
{
    fn build(&self, _request: Request<ReqBody>, reason: ResponseReason) -> Response<ResBody> {
        let status = match &reason {
            ResponseReason::Error(RateLimitError::Key(code, _)) if code == "missing_user_id" => {
                StatusCode::UNAUTHORIZED
            },
            ResponseReason::Error(RateLimitError::Key(code, _)) if code == "invalid_user_id" => StatusCode::BAD_REQUEST,
            _ => reason.status_code(),
        };

        let mut response = Response::new(ResBody::default());
        *response.status_mut() = status;
        response
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();
    let redis_url = env::var("REDIS_URL").expect("REDIS_URL must be set");
    let client = redis::Client::open(redis_url).expect("Failed to open Redis client");
    let connection = client.get_multiplexed_async_connection().await?;
    let store = RedisStore::new(connection).with_namespace("axum-redis");

    let global_limiter = RateLimitLayer::builder(IpKeyExtractor::new())
        .policy_name("global-limit")
        .with_key_encoder(|k| k.to_string())
        .with_policy(FixedWindow::new(store.clone(), 10, Duration::from_secs(60))?)
        .build()?;

    let user_limiter = RateLimitLayer::builder(UserIdKeyExtractor)
        .policy_name("user-limit")
        .with_key_encoder(|k| k.to_string())
        .response_factory(AuthResponseFactory)
        .with_policy(FixedWindow::new(store, 3, Duration::from_secs(60))?)
        .build()?;

    let auth_routes = Router::new()
        .route("/login", get(|| async { "login" }))
        .merge(Router::new().route("/me", get(|| async { "me" })).layer(user_limiter));
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .nest("/auth", auth_routes)
        .layer(global_limiter);

    let address: SocketAddr = "127.0.0.1:3000".parse()?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("listening on http://{address}");
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_user_id_is_unauthorized() {
        let response: Response<()> =
            AuthResponseFactory.build(Request::new(()), ResponseReason::Error(missing_user_id()));

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn invalid_user_id_is_bad_request() {
        let response: Response<()> =
            AuthResponseFactory.build(Request::new(()), ResponseReason::Error(invalid_user_id()));

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn unrelated_key_failure_keeps_default_server_error() {
        let response: Response<()> = AuthResponseFactory.build(
            Request::new(()),
            ResponseReason::Error(RateLimitError::Key(
                String::from("socket_ip_unavailable"),
                String::from("missing socket address"),
            )),
        );

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
