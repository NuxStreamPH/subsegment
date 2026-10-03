//! ICY (Icecast/SHOUTcast) metadata parsing.
//!
//! Two responsibilities:
//!   * header helpers — parse `icy-*` response headers into StreamMetadata;
//!   * [`IcyStreamSplitter`] — a state machine that separates inline ICY
//!     metadata blocks from audio bytes in an interleaved stream, emitting
//!     audio chunks and [`StreamMetadata`] updates through separate channels
//!     (metadata is never coupled to an output transport).

use crate::types::StreamMetadata;

/// Parse the `icy-metaint` header value.
pub fn parse_metaint(header: Option<&str>) -> Option<u32> {
    header?.trim().parse::<u32>().ok().filter(|v| *v > 0)
}

/// Extract a quoted or bare value for `Key=` inside an ICY metadata block.
pub fn icy_find_value(block: &str, key: &str) -> Option<String> {
    // Blocks look like: StreamTitle='Artist - Song';StreamUrl='';
    let pat = format!("{key}=");
    let idx = block.find(&pat)? + pat.len();
    let rest = &block[idx..];
    if rest.starts_with('\'') {
        let end = rest[1..].find('\'')? + 1;
        Some(rest[1..end].to_string())
    } else {
        let end = rest.find(';').unwrap_or(rest.len());
        Some(rest[..end].trim().to_string())
    }
}

/// Convert an inline ICY metadata block into structured metadata.
pub fn parse_icy_block(block: &str) -> StreamMetadata {
    let title = icy_find_value(block, "StreamTitle")
        .map(|t| t.trim_matches('\'').trim().to_string())
        .filter(|t| !t.is_empty());
    StreamMetadata {
        title,
        ..Default::default()
    }
}

/// Stateful splitter for interleaved ICY streams.
///
/// Feed raw upstream bytes with [`push`](Self::push); it returns audio bytes
/// (with metadata blocks removed) and any completed metadata blocks.
#[derive(Debug)]
pub struct IcyStreamSplitter {
    metaint: u32,
    /// Audio bytes remaining before the next metadata block.
    audio_remaining: u64,
    /// Bytes of the current metadata block still to read (header first).
    meta_remaining: usize,
    /// Header byte of metadata length (metaint units).
    in_meta_header: bool,
    meta_buf: Vec<u8>,
}

/// Outcome of feeding bytes into the splitter.
#[derive(Debug, Default)]
pub struct SplitOutput {
    pub audio: Vec<u8>,
    pub metadata: Vec<StreamMetadata>,
}

impl IcyStreamSplitter {
    pub fn new(metaint: u32) -> Self {
        Self {
            metaint,
            audio_remaining: metaint as u64,
            meta_remaining: 0,
            in_meta_header: false,
            meta_buf: Vec::new(),
        }
    }

    pub fn push(&mut self, data: &[u8]) -> SplitOutput {
        let mut out = SplitOutput::default();
        let mut i = 0usize;
        while i < data.len() {
            if self.audio_remaining == 0 {
                // A metadata section starts here: 1 length byte, then
                // len*16 bytes of ASCII metadata.
                self.in_meta_header = true;
                self.meta_buf.clear();
            }
            if self.in_meta_header {
                let b = data[i];
                i += 1;
                self.in_meta_header = false;
                let size = (b as usize) * 16;
                if size == 0 {
                    self.audio_remaining = self.metaint as u64;
                    continue;
                }
                self.meta_remaining = size;
                continue;
            }
            if self.meta_remaining > 0 {
                let take = self.meta_remaining.min(data.len() - i);
                self.meta_buf.extend_from_slice(&data[i..i + take]);
                self.meta_remaining -= take;
                i += take;
                if self.meta_remaining == 0 {
                    let block = String::from_utf8_lossy(&self.meta_buf);
                    let md = parse_icy_block(block.trim_end_matches('\0'));
                    if !md.is_empty() {
                        out.metadata.push(md);
                    }
                    self.meta_buf.clear();
                    self.audio_remaining = self.metaint as u64;
                }
                continue;
            }
            // Copy audio up to the next boundary.
            let want = self.audio_remaining.min((data.len() - i) as u64) as usize;
            out.audio.extend_from_slice(&data[i..i + want]);
            self.audio_remaining -= want as u64;
            i += want;
        }
        out
    }
}

/// Build initial metadata from upstream HTTP response headers.
pub fn metadata_from_headers(
    content_type: Option<&str>,
    name: Option<&str>,
    bitrate: Option<&str>,
    genre: Option<&str>,
) -> StreamMetadata {
    StreamMetadata {
        title: None,
        station: name.map(|s| s.to_string()).or_else(|| genre.map(|s| s.to_string())),
        codec: codec_from_content_type(content_type),
        bitrate: bitrate.and_then(|b| b.parse::<u32>().ok()),
        content_type: content_type.map(|s| s.to_string()),
    }
}

/// Best-effort codec sniffing from a MIME type.
pub fn codec_from_content_type(ct: Option<&str>) -> Option<String> {
    let ct = ct?.to_ascii_lowercase();
    let c = if ct.contains("mpeg") || ct.contains("mp3") {
        "mp3"
    } else if ct.contains("opus") {
        "opus"
    } else if ct.contains("aac") || ct.contains("adts") {
        "aac"
    } else if ct.contains("ogg") {
        "ogg"
    } else if ct.contains("m4a") || ct.contains("mp4") {
        "aac"
    } else if ct.contains("flac") {
        "flac"
    } else {
        return None;
    };
    Some(c.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_values() {
        let block = "StreamTitle='Nux FM - Test Song';StreamUrl='http://x';";
        assert_eq!(
            icy_find_value(block, "StreamTitle").as_deref(),
            Some("Nux FM - Test Song")
        );
        assert_eq!(icy_find_value(block, "StreamUrl").as_deref(), Some("http://x"));
        assert_eq!(icy_find_value(block, "Nope"), None);
    }

    #[test]
    fn parse_block_structured() {
        let m = parse_icy_block("StreamTitle='Artist - Title';");
        assert_eq!(m.title.as_deref(), Some("Artist - Title"));
        assert!(m.station.is_none());
    }

    #[test]
    fn splitter_separates_audio_and_metadata() {
        let metaint = 8u32;
        let mut sp = IcyStreamSplitter::new(metaint);

        // Build a synthetic interleaved stream:
        // [8 audio][len=1][16-byte meta "StreamTitle='Hi';"][8 audio]...
        let mut wire: Vec<u8> = Vec::new();
        wire.extend(b"Aaaaaaaa"); // 8 audio
        let meta = b"StreamTitle='Hi';"; // 17 chars -> pad to 32 (2 units)
        let padded = {
            let mut v = meta.to_vec();
            while v.len() % 16 != 0 {
                v.push(0);
            }
            v
        };
        wire.push((padded.len() / 16) as u8);
        wire.extend(&padded);
        wire.extend(b"Bbbbbbbb"); // 8 audio

        // Feed one byte at a time to exercise all states.
        let mut audio = Vec::new();
        let mut metas = Vec::new();
        for chunk in wire.chunks(1) {
            let o = sp.push(chunk);
            audio.extend(o.audio);
            metas.extend(o.metadata);
        }
        assert_eq!(audio, b"AaaaaaaaBbbbbbbb".to_vec());
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].title.as_deref(), Some("Hi"));
    }

    #[test]
    fn splitter_handles_empty_meta_section() {
        let mut sp = IcyStreamSplitter::new(4);
        let o = sp.push(b"abcd\x00efgh");
        assert_eq!(o.audio, b"abcdefgh".to_vec());
        assert!(o.metadata.is_empty());
    }

    #[test]
    fn codec_sniff() {
        assert_eq!(codec_from_content_type("audio/mp3").as_deref(), Some("mp3"));
        assert_eq!(codec_from_content_type("audio/ogg; codecs=opus").as_deref(), Some("opus"));
        assert_eq!(codec_from_content_type("video/mp4").as_deref(), Some("aac"));
        assert_eq!(codec_from_content_type(None), None);
    }

    #[test]
    fn headers_to_metadata() {
        let m = metadata_from_headers(Some("audio/mpeg"), Some("Nux FM"), Some("128"), None);
        assert_eq!(m.station.as_deref(), Some("Nux FM"));
        assert_eq!(m.bitrate, Some(128));
        assert_eq!(m.codec.as_deref(), Some("mp3"));
    }
}
