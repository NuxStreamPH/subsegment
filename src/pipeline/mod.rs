//! Shared streaming pipelines.
//!
//! A pipeline is one ingest (+ optional transcode) chain feeding many
//! listeners. Pipeline identity is [`StreamSpec::key()`]; concurrent requests
//! that resolve to the same key reuse a single upstream connection and (when
//! transcoding) a single encoder process.

pub mod fanout;
pub mod manager;

pub use fanout::{FanEvent, Fanout, Subscriber};
pub use manager::{Attach, PipelineManager};
