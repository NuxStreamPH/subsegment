//! HLS source adapter.
//!
//! v1 strategy: treat the configured URL as a directly playable media
//! stream (many HLS publishers expose a continuous AAC/TS byte stream at
//! the playlist's media URI, and FFmpeg itself consumes `.m3u8` URLs).
//! When the URL points at a playlist we hand it to the transcoder backend,
//! which demuxes segments for us; passthrough is disabled for playlists.

use std::sync::Arc;

use tokio::sync::{mpsc, Notify};

use crate::config::{AppConfig, BroadcastConfig};
use crate::error::Result;
use crate::types::StreamMetadata;

use super::http::{spawn_ingest, UpstreamInfo};

/// Heuristic: does this URL look like an `.m3u8` playlist?
pub fn looks_like_playlist(url: &str) -> bool {
    let path = url.split('?').next().unwrap_or(url);
    path.ends_with(".m3u8") || path.ends_with(".m3u")
}

pub async fn connect(
    cfg: Arc<AppConfig>,
    client: reqwest::Client,
    mountpoint: String,
    bc: BroadcastConfig,
    bytes_tx: mpsc::Sender<Vec<u8>>,
    meta_tx: mpsc::Sender<StreamMetadata>,
    cancel: Arc<Notify>,
) -> Result<(UpstreamInfo, tokio::task::JoinHandle<()>)> {
    spawn_ingest(cfg, client, mountpoint, bc, bytes_tx, meta_tx, cancel).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn playlist_detection() {
        assert!(looks_like_playlist("https://x.example/live/master.m3u8"));
        assert!(looks_like_playlist("https://x.example/a.m3u8?token=1"));
        assert!(!looks_like_playlist("https://x.example/live.aac"));
    }
}
