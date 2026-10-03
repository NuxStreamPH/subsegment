//! Request/correlation ID propagation.

use axum::extract::Request;
use axum::http::header;
use uuid::Uuid;

pub const REQUEST_ID: header::HeaderName = header::HeaderName::from_static("x-request-id");

/// Axum middleware: accept an inbound `X-Request-Id` or mint one, store it
/// in request extensions, echo it on the response and attach it to log
/// spans. Never contains credentials.
pub async fn middleware(mut req: Request, next: axum::middleware::Next) -> axum::response::Response {
    let id = req
        .headers()
        .get(&REQUEST_ID)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty() && s.len() <= 128 && s.chars().all(|c| c.is_alphanumeric() || "-_.".contains(c)))
        .map(|s| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    req.extensions_mut().insert(RequestId(id.clone()));
    let mut res = next.run(req).await;
    if let Ok(v) = axum::http::HeaderValue::from_str(&id) {
        res.headers_mut().insert(&REQUEST_ID, v);
    }
    res
}

#[derive(Clone, Debug)]
pub struct RequestId(pub String);

impl RequestId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
