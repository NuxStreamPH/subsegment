//! Security prechecks: SSRF protection for upstreams plus resource limits.

pub mod limits;
pub mod upstream;

pub use limits::LimitGuard;
pub use upstream::{check_upstream_url, UpstreamCheckError};
