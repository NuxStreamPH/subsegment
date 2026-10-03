//! NuxStream Stream Engine — library root.
//!
//! Production-oriented backend that ingests live broadcast streams,
//! normalizes/transcodes them and serves authenticated HTTP listeners
//! through shared pipelines.

pub mod auth;
pub mod config;
pub mod error;
pub mod metadata;
pub mod security;
pub mod transcoder;
pub mod types;

pub use error::EngineError;
pub use types::{Codec, Quality, SourceType, StreamMetadata, StreamSpec};
