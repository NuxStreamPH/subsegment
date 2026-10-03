//! Core domain types shared across the stream engine.
//!
//! These types intentionally have no dependency on HTTP, FFmpeg or any
//! particular transport so that subsystems stay loosely coupled.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Quality profile requested by a listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Quality {
    /// ~48-64 kbps, optimized for mobile / constrained connections.
    Low,
    /// ~96-128 kbps, balanced.
    Medium,
    /// ~160-256 kbps, high quality.
    High,
    /// Pass the upstream stream through untouched whenever possible.
    Original,
}

impl Quality {
    pub fn as_str(&self) -> &'static str {
        match self {
            Quality::Low => "low",
            Quality::Medium => "medium",
            Quality::High => "high",
            Quality::Original => "original",
        }
    }
}

impl fmt::Display for Quality {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Quality {
    type Err = ParseTypeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "low" => Ok(Quality::Low),
            "medium" => Ok(Quality::Medium),
            "high" => Ok(Quality::High),
            "original" => Ok(Quality::Original),
            other => Err(ParseTypeError(other.to_string())),
        }
    }
}

/// Output codec selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Codec {
    /// Let the server choose based on configuration and quality.
    Auto,
    Opus,
    Mp3,
    Aac,
    /// AAC+ (HE-AAC / SBR).
    AacPlus,
}

impl Codec {
    pub fn as_str(&self) -> &'static str {
        match self {
            Codec::Auto => "auto",
            Codec::Opus => "opus",
            Codec::Mp3 => "mp3",
            Codec::Aac => "aac",
            Codec::AacPlus => "aacplus",
        }
    }

    /// MIME content type appropriate for HTTP responses.
    pub fn content_type(&self) -> &'static str {
        match self {
            Codec::Auto | Codec::Opus => "audio/webm",
            Codec::Mp3 => "audio/mpeg",
            Codec::Aac | Codec::AacPlus => "audio/aac",
        }
    }
}

impl fmt::Display for Codec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Codec {
    type Err = ParseTypeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "auto" => Ok(Codec::Auto),
            "opus" => Ok(Codec::Opus),
            "mp3" => Ok(Codec::Mp3),
            "aac" => Ok(Codec::Aac),
            "aacplus" | "aac+" | "he-aac" => Ok(Codec::AacPlus),
            other => Err(ParseTypeError(other.to_string())),
        }
    }
}

/// Upstream source kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    Icecast,
    Shoutcast,
    Hls,
    /// Generic HTTP(S) audio stream.
    Http,
}

impl fmt::Display for SourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            SourceType::Icecast => "icecast",
            SourceType::Shoutcast => "shoutcast",
            SourceType::Hls => "hls",
            SourceType::Http => "http",
        };
        f.write_str(s)
    }
}

impl FromStr for SourceType {
    type Err = ParseTypeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "icecast" => Ok(SourceType::Icecast),
            "shoutcast" => Ok(SourceType::Shoutcast),
            "hls" => Ok(SourceType::Hls),
            "http" | "https" | "generic" => Ok(SourceType::Http),
            other => Err(ParseTypeError(other.to_string())),
        }
    }
}

/// Error returned when a quality/codec/source string cannot be parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseTypeError(pub String);

impl fmt::Display for ParseTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unsupported value: {}", self.0)
    }
}

impl std::error::Error for ParseTypeError {}

/// Stream metadata preserved from the upstream source.
///
/// Metadata updates are distributed separately from the raw audio pipeline
/// (see `pipeline::manager`), so parsing is never tightly coupled to one
/// output transport.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamMetadata {
    pub title: Option<String>,
    pub station: Option<String>,
    pub codec: Option<String>,
    pub bitrate: Option<u32>,
    pub content_type: Option<String>,
}

impl StreamMetadata {
    /// Merge newer metadata over older values, keeping fields that the
    /// update does not carry.
    pub fn merge(&self, update: &StreamMetadata) -> StreamMetadata {
        StreamMetadata {
            title: update.title.clone().or_else(|| self.title.clone()),
            station: update.station.clone().or_else(|| self.station.clone()),
            codec: update.codec.clone().or_else(|| self.codec.clone()),
            bitrate: update.bitrate.or(self.bitrate),
            content_type: update.content_type.clone().or_else(|| self.content_type.clone()),
        }
    }

    /// True when every field is absent.
    pub fn is_empty(&self) -> bool {
        self.title.is_none()
            && self.station.is_none()
            && self.codec.is_none()
            && self.bitrate.is_none()
            && self.content_type.is_none()
    }
}

/// Fully resolved representation a listener will receive.
///
/// Pipeline identity is `(broadcast, quality, codec)` plus any relevant
/// transcoding parameters; two requests resolving to the same `StreamSpec`
/// share one pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamSpec {
    pub broadcast: String,
    pub quality: Quality,
    /// The concrete output codec (never `Auto`).
    pub codec: Codec,
    /// Target bitrate in bits per second; `None` means passthrough/original.
    pub bitrate_bps: Option<u32>,
    /// True when the upstream can be relayed without re-encoding.
    pub passthrough: bool,
}

impl StreamSpec {
    /// Stable key used by the pipeline manager for reuse/deduplication.
    pub fn key(&self) -> String {
        match self.bitrate_bps {
            Some(b) => format!("{}|{}|{}|{}", self.broadcast, self.quality, self.codec, b),
            None => format!("{}|{}|{}|orig", self.broadcast, self.quality, self.codec),
        }
    }
}

impl fmt::Display for StreamSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}[{}@{}{}]",
            self.broadcast,
            self.quality,
            self.codec,
            self.bitrate_bps
                .map(|b| format!(":{k}bps", k = b / 1000))
                .unwrap_or_default()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_quality() {
        assert_eq!("low".parse::<Quality>().unwrap(), Quality::Low);
        assert_eq!("MEDIUM".parse::<Quality>().unwrap(), Quality::Medium);
        assert_eq!("Original".parse::<Quality>().unwrap(), Quality::Original);
        assert!("ultra".parse::<Quality>().is_err());
    }

    #[test]
    fn parse_codec() {
        assert_eq!("opus".parse::<Codec>().unwrap(), Codec::Opus);
        assert_eq!("aacplus".parse::<Codec>().unwrap(), Codec::AacPlus);
        assert_eq!("HE-AAC".parse::<Codec>().unwrap(), Codec::AacPlus);
        assert!("flac".parse::<Codec>().is_err());
    }

    #[test]
    fn spec_key_distinguishes_bitrate() {
        let a = StreamSpec {
            broadcast: "x".into(),
            quality: Quality::Medium,
            codec: Codec::Opus,
            bitrate_bps: Some(128_000),
            passthrough: false,
        };
        let b = StreamSpec {
            bitrate_bps: Some(96_000),
            ..a.clone()
        };
        assert_ne!(a.key(), b.key());
        assert_eq!(a.key(), a.clone().key());
    }

    #[test]
    fn metadata_merge_keeps_unset_fields() {
        let old = StreamMetadata {
            title: Some("old".into()),
            station: Some("Nux".into()),
            codec: None,
            bitrate: Some(128),
            content_type: None,
        };
        let upd = StreamMetadata {
            title: Some("new".into()),
            ..Default::default()
        };
        let m = old.merge(&upd);
        assert_eq!(m.title.as_deref(), Some("new"));
        assert_eq!(m.station.as_deref(), Some("Nux"));
        assert_eq!(m.bitrate, Some(128));
    }
}
