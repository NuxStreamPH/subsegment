//! Icecast source adapter.
//!
//! Icecast mounts advertise interleaved ICY metadata when the client sends
//! `Icy-MetaData: 1`; the shared HTTP pump already does this for sources
//! configured with `type: icecast`.

use std::sync::Arc;

use tokio::sync::{mpsc, Notify};

use crate::config::{AppConfig, BroadcastConfig};
use crate::error::Result;
use crate::types::StreamMetadata;

use super::http::{spawn_ingest, UpstreamInfo};

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
