//! Stream metadata handling.

pub mod icy;

pub use crate::types::StreamMetadata as Metadata;
pub use icy::{
    codec_from_content_type, icy_find_value, metadata_from_headers, parse_icy_block,
    parse_metaint, IcyStreamSplitter,
};
