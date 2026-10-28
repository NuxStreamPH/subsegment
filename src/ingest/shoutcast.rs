//! SHOUTcast source adapter — same wire protocol family as Icecast (ICY
//! headers + interleaved metadata), kept separate so SHOUTcast v2 JSON
//! directory/metadata extensions can be added without touching Icecast code.

use tokio_util::sync::CancellationToken;

use std::sync::Arc;

use tokio::sync::mpsc;

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
    cancel: CancellationToken,
) -> Result<(UpstreamInfo, tokio::task::JoinHandle<()>)> {
    spawn_ingest(cfg, client, mountpoint, bc, bytes_tx, meta_tx, cancel).await
}
