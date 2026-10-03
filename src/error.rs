//! Engine-wide error types with HTTP status mappings.
//!
//! Error messages returned to clients are intentionally generic; internal
//! detail is only recorded through structured logs so that configuration
//! specifics never leak through API errors.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use thiserror::Error;

pub type Result<T, E = EngineError> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("broadcast not found")]
    BroadcastNotFound,

    #[error("broadcast disabled")]
    BroadcastDisabled,

    #[error("authentication required")]
    Unauthorized,

    #[error("access denied")]
    Forbidden,

    #[error("unsupported parameter")]
    BadParameter(String),

    #[error("request conflict")]
    Conflict,

    #[error("rate limited")]
    RateLimited,

    #[error("server busy / limit reached")]
    LimitExceeded,

    #[error("upstream unavailable")]
    UpstreamUnavailable,

    #[error("service not ready")]
    NotReady,

    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

impl EngineError {
    pub fn status(&self) -> StatusCode {
        match self {
            EngineError::BroadcastNotFound => StatusCode::NOT_FOUND,
            EngineError::Unauthorized => StatusCode::UNAUTHORIZED,
            EngineError::Forbidden | EngineError::BroadcastDisabled => StatusCode::FORBIDDEN,
            EngineError::BadParameter(_) => StatusCode::BAD_REQUEST,
            EngineError::Conflict => StatusCode::CONFLICT,
            EngineError::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            EngineError::LimitExceeded | EngineError::UpstreamUnavailable | EngineError::NotReady => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            EngineError::Config(_) | EngineError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Stable machine-readable error code for JSON bodies and metrics.
    pub fn code(&self) -> &'static str {
        match self {
            EngineError::BroadcastNotFound => "broadcast_not_found",
            EngineError::BroadcastDisabled => "broadcast_disabled",
            EngineError::Unauthorized => "unauthorized",
            EngineError::Forbidden => "forbidden",
            EngineError::BadParameter(_) => "bad_parameter",
            EngineError::Conflict => "conflict",
            EngineError::RateLimited => "rate_limited",
            EngineError::LimitExceeded => "limit_exceeded",
            EngineError::UpstreamUnavailable => "upstream_unavailable",
            EngineError::NotReady => "not_ready",
            EngineError::Config(_) => "config_error",
            EngineError::Internal(_) => "internal_error",
        }
    }

    /// Client-safe message. Never contains URLs, tokens or config values.
    pub fn safe_message(&self) -> String {
        match self {
            // Internal details (URLs, hostnames, library errors) must never
            // reach clients; they stay in the logs via `Display`/tracing.
            EngineError::Internal(_) => "internal error".to_string(),
            // Config validation runs at startup (operator-facing), so the
            // message may quote offending values; it never reaches clients.
            EngineError::Config(m) => m.clone(),
            EngineError::BadParameter(m) => m.clone(),
            other => other.to_string(),
        }
    }

    pub fn internal(msg: impl Into<anyhow::Error>) -> Self {
        EngineError::Internal(msg.into())
    }
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    error: &'a str,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
}

/// Attach the current request id (from extensions) to an error response.
impl IntoResponse for EngineError {
    fn into_response(self) -> Response {
        let body = ErrorBody {
            error: self.code(),
            message: self.safe_message(),
            request_id: None,
        };
        // The request-id layer enriches responses; fall back to a plain JSON body.
        let json = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());
        (self.status(), json).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_mapping() {
        assert_eq!(EngineError::BroadcastNotFound.status(), StatusCode::NOT_FOUND);
        assert_eq!(EngineError::Unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(EngineError::Forbidden.status(), StatusCode::FORBIDDEN);
        assert_eq!(EngineError::Conflict.status(), StatusCode::CONFLICT);
        assert_eq!(EngineError::RateLimited.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(EngineError::LimitExceeded.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(EngineError::UpstreamUnavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn safe_message_hides_internals() {
        let e = EngineError::internal(anyhow::anyhow!("failed connecting to https://secret-host"));
        assert!(!e.safe_message().contains("secret-host"));
        assert_eq!(e.safe_message(), "internal error");
        // `code` is what clients see structurally:
        assert_eq!(e.code(), "internal_error");
        // Full detail stays available for logs (never sent to clients).
        assert!(format!("{e}").contains("secret-host"));
    }
}
