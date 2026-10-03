//! Upstream ingest: connect to broadcast sources, detect container/codec,
//! extract ICY metadata and feed a bounded byte channel with automatic
//! reconnection and capped exponential backoff.

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::{mpsc, Notify};
use tracing::{debug, info, warn};

use crate::config::{AppConfig, BroadcastConfig};
use crate::error::{EngineError, Result};
use crate::metadata::icy::{self, IcyStreamSplitter};
use crate::types::{Codec, SourceType, StreamMetadata};

/// What we learned about the upstream when the connection was established.
#[derive(Debug, Clone)]
pub struct UpstreamInfo {
    pub content_type: Option<String>,
    pub codec: Option<Codec>,
    /// kbps as advertised by the source (if any).
    pub bitrate_kbps: Option<u32>,
    pub icy_metaint: Option<u32>,
}

impl UpstreamInfo {
    /// The codec we could relay unchanged for ORIGINAL quality.
    pub fn passthrough_codec(&self) -> Option<Codec> {
        self.codec
    }
}

/// Build the reqwest client used for all upstreams. Redirects are followed
/// but every destination is revalidated against the SSRF policy.
pub fn build_http_client(cfg: &AppConfig) -> Result<reqwest::Client> {
    let security_cfg = cfg.clone();
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(
            cfg.limits.upstream_connect_timeout_secs,
        ))
        .timeout(Duration::from_secs(
            cfg.limits.upstream_read_timeout_secs.saturating_mul(4),
        ))
        .redirect(reqwest::redirect::Policy::custom(move |attempt| {
            if attempt.previous().len() >= security_cfg.limits.upstream_max_redirects {
                return attempt.error("too many redirects");
            }
            // Revalidate the redirect target under the same network policy.
            match crate::security::upstream::check_upstream_url(
                &security_cfg,
                attempt.url().as_str(),
            ) {
                Ok(_) => attempt.follow(),
                Err(_) => attempt.error("redirect blocked by network policy"),
            }
        }))
        .build()
        .map_err(EngineError::internal)
}

fn initial_metadata(bc: &BroadcastConfig, info: &UpstreamInfo) -> StreamMetadata {
    StreamMetadata {
        title: None,
        station: bc.station_name.clone(),
        codec: info
            .content_type
            .clone()
            .or_else(|| info.codec.map(|c| c.as_str().to_string())),
        bitrate: info.bitrate_kbps,
        content_type: info.content_type.clone(),
    }
}

/// Connect once; returns negotiated info, the response (body not consumed
/// yet) and an ICY splitter if the source advertises interleaved metadata.
async fn open_once(
    client: &reqwest::Client,
    bc: &BroadcastConfig,
    want_icy: bool,
) -> Result<(UpstreamInfo, reqwest::Response, Option<IcyStreamSplitter>)> {
    let mut rb = client.get(&bc.source.url);
    if want_icy {
        rb = rb.header("Icy-MetaData", "1");
    }
    for (k, v) in &bc.source.headers {
        rb = rb.header(k.as_str(), v.as_str());
    }
    let resp = rb
        .send()
        .await
        .map_err(|_| EngineError::UpstreamUnavailable)?;
    let status = resp.status();
    if !status.is_success() {
        debug!(%status, "upstream returned error status");
        return Err(EngineError::UpstreamUnavailable);
    }

    let headers = resp.headers().clone();
    let get = |n: &str| {
        headers
            .get(n)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    };

    let content_type = get("content-type");
    let metaint = icy::parse_metaint(get("icy-metaint").as_deref());
    let splitter = if want_icy {
        metaint.map(IcyStreamSplitter::new)
    } else {
        None
    };

    let codec = icy::codec_from_content_type(content_type.as_deref())
        .as_deref()
        .and_then(|s| s.parse::<Codec>().ok());
    let bitrate_kbps = get("icy-br")
        .and_then(|b| b.trim().parse::<u32>().ok())
        .or_else(|| {
            content_type.as_ref().and_then(|ct| {
                ct.split(';')
                    .find_map(|p| p.trim().strip_prefix("bitrate="))
                    .and_then(|b| b.parse::<u32>().ok())
            })
        });

    Ok((
        UpstreamInfo {
            content_type,
            codec,
            bitrate_kbps,
            icy_metaint: metaint,
        },
        resp,
        splitter,
    ))
}

async fn open_with_retries(
    client: &reqwest::Client,
    bc: &BroadcastConfig,
    want_icy: bool,
    cancel: &Arc<Notify>,
) -> Result<(UpstreamInfo, reqwest::Response, Option<IcyStreamSplitter>)> {
    let mut delay_ms = 250u64;
    loop {
        match open_once(client, bc, want_icy).await {
            Ok(v) => return Ok(v),
            Err(e) => {
                let slept = tokio::time::sleep(Duration::from_millis(delay_ms));
                tokio::select! {
                    _ = cancel.notified() => return Err(EngineError::UpstreamUnavailable),
                    _ = slept => {}
                }
                delay_ms = (delay_ms * 2).min(8_000);
                debug!(error = %e, "upstream connect failed, retrying");
            }
        }
    }
}

/// Spawn the ingest loop with caller-owned channels. Returns negotiated
/// upstream info after the first successful connect; on disconnect the loop
/// reconnects with backoff until `cancel` fires or the byte sender closes.
pub async fn spawn_ingest(
    cfg: Arc<AppConfig>,
    client: reqwest::Client,
    mountpoint: String,
    bc: BroadcastConfig,
    bytes_tx: mpsc::Sender<Vec<u8>>,
    meta_tx: mpsc::Sender<StreamMetadata>,
    cancel: Arc<Notify>,
) -> Result<(UpstreamInfo, tokio::task::JoinHandle<()>)> {
    let want_icy = matches!(bc.source.r#type, SourceType::Icecast | SourceType::Shoutcast);
    let (info, resp, splitter) = open_with_retries(&client, &bc, want_icy, &cancel).await?;
    info!(%mountpoint, codec = ?info.codec, metaint = ?info.icy_metaint, "upstream connected");
    let _ = meta_tx.send(initial_metadata(&bc, &info)).await;
    let join = tokio::spawn(pump_loop(
        cfg,
        client,
        bc,
        mountpoint,
        info.clone(),
        Some(resp),
        splitter,
        bytes_tx,
        meta_tx,
        cancel,
    ));
    Ok((info, join))
}

#[allow(clippy::too_many_arguments)]
async fn pump_loop(
    cfg: Arc<AppConfig>,
    client: reqwest::Client,
    bc: BroadcastConfig,
    mountpoint: String,
    _initial_info: UpstreamInfo,
    mut pending: Option<reqwest::Response>,
    mut splitter: Option<IcyStreamSplitter>,
    bytes_tx: mpsc::Sender<Vec<u8>>,
    meta_tx: mpsc::Sender<StreamMetadata>,
    cancel: Arc<Notify>,
) {
    let want_icy = matches!(bc.source.r#type, SourceType::Icecast | SourceType::Shoutcast);
    let read_timeout = cfg.limits.upstream_read_timeout();

    loop {
        // Obtain a body: either the one we already hold or a fresh connect.
        let body = match pending.take() {
            Some(r) => r.bytes_stream(),
            None => {
                match open_with_retries(&client, &bc, want_icy, &cancel).await {
                    Ok((i, r, s)) => {
                        crate::telemetry::metrics::UPSTREAM_RECONNECTS
                            .with_label_values(&[&mountpoint])
                            .inc();
                        info = i;
                        splitter = s;
                        let _ = meta_tx.send(initial_metadata(&bc, &info)).await;
                        r.bytes_stream()
                    }
                    Err(_) => break, // cancelled while retrying
                }
            }
        };
        futures_util::pin_mut!(body);

        let need_reconnect;
        loop {
            let next = tokio::select! {
                biased;
                _ = cancel.notified() => { need_reconnect = false; break; }
                chunk = tokio::time::timeout(read_timeout, body.next()) => chunk,
            };
            match next {
                Err(_) => {
                    warn!(%mountpoint, "upstream read timeout — reconnecting");
                    need_reconnect = true;
                    break;
                }
                Ok(None) => {
                    info!(%mountpoint, "upstream closed — reconnecting");
                    need_reconnect = true;
                    break;
                }
                Ok(Some(Err(_))) => {
                    warn!(%mountpoint, "upstream read error — reconnecting");
                    need_reconnect = true;
                    break;
                }
                Ok(Some(Ok(b))) => {
                    crate::telemetry::metrics::BYTES_RECEIVED
                        .with_label_values(&[&mountpoint])
                        .inc_by(b.len() as f64);
                    let out = match splitter.as_mut() {
                        Some(sp) => sp.push(&b),
                        None => icy::SplitOutput {
                            audio: b.to_vec(),
                            metadata: Vec::new(),
                        },
                    };
                    for md in out.metadata {
                        let _ = meta_tx.send(md).await;
                    }
                    if !out.audio.is_empty()
                        && bytes_tx.send(out.audio).await.is_err()
                    {
                        return; // pipeline consumer gone
                    }
                }
            }
        }
        if !need_reconnect && pending.is_none() {
            // Cancelled mid-body: exit cleanly.
            break;
        }
    }
    debug!(%mountpoint, "ingest loop terminated");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_codec_maps() {
        let i = UpstreamInfo {
            content_type: Some("audio/mpeg".into()),
            codec: Some(Codec::Mp3),
            bitrate_kbps: Some(128),
            icy_metaint: None,
        };
        assert_eq!(i.passthrough_codec(), Some(Codec::Mp3));
    }
}
