//! HTTP API surface.

pub mod health;
pub mod routes;
pub mod stream;

pub use routes::{build_router, AppState};
