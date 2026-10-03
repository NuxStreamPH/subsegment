//! Transcoding backend abstraction.
//!
//! The rest of the application depends only on the [`Transcoder`] trait and
//! these data types — never on FFmpeg command construction. Swapping in
//! native Rust codec pipelines (e.g. `NativeOpusTranscoder`) later requires
//! only a new implementor.

pub mod ffmpeg;
pub mod passthrough;
pub mod traits;

pub use ffmpeg::FfmpegTranscoder;
pub use passthrough::PassthroughTranscoder;
pub use traits::{EncodingProfile, StreamInput, TranscodeError, TranscodedStream, Transcoder};

use crate::types::Codec;

/// Which container/codec an output profile requests from the backend.
pub fn container_for(codec: Codec) -> &'static str {
    match codec {
        Codec::Opus => "webm",
        Codec::Mp3 => "mp3",
        Codec::Aac | Codec::AacPlus => "adts",
        Codec::Auto => "mp3",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn containers() {
        assert_eq!(container_for(Codec::Opus), "webm");
        assert_eq!(container_for(Codec::Mp3), "mp3");
        assert_eq!(container_for(Codec::Aac), "adts");
    }
}
