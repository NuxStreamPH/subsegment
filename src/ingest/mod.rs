//! Source-type adapters.
//!
//! All supported source types (Icecast, SHOUTcast, HLS, generic HTTP) share
//! the same reqwest-based pump in [`http`]; the differences are which ICY
//! headers we request and how redirects/metadata are interpreted. Keeping
//! them as thin wrappers preserves a clean extension point for future
//! source protocols (e.g. SRT) without touching the pipeline layer.

pub mod hls;
pub mod http;
pub mod icecast;
pub mod shoutcast;

pub use http::{build_http_client, spawn_ingest, UpstreamInfo};
