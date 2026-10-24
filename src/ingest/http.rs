//! Upstream ingest: connect to broadcast sources, detect container/codec,
//! extract ICY metadata and feed a bounded byte channel with automatic
//! reconnection and capped exponential backoff.

use tokio_util::sync::CancellationToken;

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::mpsc;
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
        // A live response never finishes. A total request deadline disconnects
        // healthy stations periodically; only time out stalled reads/headers.
        .read_timeout(cfg.limits.upstream_read_timeout())
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
        .map_err(|err| {
            debug!(is_timeout = err.is_timeout(), error = %err.without_url(), "upstream request failed");
            EngineError::UpstreamUnavailable
        })?;
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
    cancel: &CancellationToken,
) -> Result<(UpstreamInfo, reqwest::Response, Option<IcyStreamSplitter>)> {
    let mut delay_ms = 250u64;
    loop {
        let opened = tokio::select! {
            _ = cancel.cancelled() => return Err(EngineError::UpstreamUnavailable),
            result = open_once(client, bc, want_icy) => result,
        };
        match opened {
            Ok(v) => return Ok(v),
            Err(e) => {
                let slept = tokio::time::sleep(retry_delay(delay_ms));
                tokio::select! {
                    _ = cancel.cancelled() => return Err(EngineError::UpstreamUnavailable),
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
    cancel: CancellationToken,
) -> Result<(UpstreamInfo, tokio::task::JoinHandle<()>)> {
    let want_icy = matches!(
        bc.source.r#type,
        SourceType::Icecast | SourceType::Shoutcast
    );
    let (info, resp, splitter) = open_with_retries(&client, &bc, want_icy, &cancel).await?;
    info!(%mountpoint, codec = ?info.codec, metaint = ?info.icy_metaint, "upstream connected");
    let _ = meta_tx.send(initial_metadata(&bc, &info)).await;
    let join = tokio::spawn(pump_loop(
        cfg,
        client,
        bc,
        mountpoint,
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
    mut pending: Option<reqwest::Response>,
    mut splitter: Option<IcyStreamSplitter>,
    bytes_tx: mpsc::Sender<Vec<u8>>,
    meta_tx: mpsc::Sender<StreamMetadata>,
    cancel: CancellationToken,
) {
    let want_icy = matches!(
        bc.source.r#type,
        SourceType::Icecast | SourceType::Shoutcast
    );
    let read_timeout = cfg.limits.upstream_read_timeout();
    let mut reconnect_delay_ms = 250u64;

    loop {
        // Obtain a body: either the one we already hold or a fresh connect.
        let mut body = Box::pin(match pending.take() {
            Some(r) => r.bytes_stream(),
            None => {
                match open_with_retries(&client, &bc, want_icy, &cancel).await {
                    Ok((i, r, s)) => {
                        crate::telemetry::metrics::UPSTREAM_RECONNECTS
                            .with_label_values(&[&mountpoint])
                            .inc();
                        splitter = s;
                        let _ = meta_tx.try_send(initial_metadata(&bc, &i));
                        r.bytes_stream()
                    }
                    Err(_) => break, // cancelled while retrying
                }
            }
        });
        let connection = ActiveConnection::new(&mountpoint);
        let connected_at = tokio::time::Instant::now();

        let need_reconnect;
        loop {
            let next = tokio::select! {
                biased;
                _ = cancel.cancelled() => { need_reconnect = false; break; }
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
                Ok(Some(Err(err))) => {
                    warn!(%mountpoint, is_timeout = err.is_timeout(), error = %err.without_url(), "upstream read error — reconnecting");
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
                        let _ = meta_tx.try_send(md);
                    }
                    if !out.audio.is_empty() {
                        // Bound every chunk as well as the number of queued chunks.
                        for chunk in out.audio.chunks(64 * 1024) {
                            tokio::select! {
                                _ = cancel.cancelled() => return,
                                result = bytes_tx.send(chunk.to_vec()) => if result.is_err() { return; },
                            }
                        }
                    }
                }
            }
        }
        if !need_reconnect && pending.is_none() {
            // Cancelled mid-body: exit cleanly.
            break;
        }
        crate::telemetry::metrics::UPSTREAM_FAILURES
            .with_label_values(&[&mountpoint])
            .inc();
        // Back off even if the peer accepts HTTP and immediately closes its
        // body. Reset only after a stable session, not after one audio chunk.
        if connected_at.elapsed() >= Duration::from_secs(30) {
            reconnect_delay_ms = 250;
        }
        drop(body);
        drop(connection);
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(retry_delay(reconnect_delay_ms)) => {}
        }
        reconnect_delay_ms = (reconnect_delay_ms * 2).min(8_000);
    }
    debug!(%mountpoint, "ingest loop terminated");
}

fn retry_delay(base_ms: u64) -> Duration {
    // Add bounded jitter so stations recovering together do not reconnect in
    // lockstep. No RNG is held across an await.
    Duration::from_millis(base_ms + rand::random::<u64>() % (base_ms / 4 + 1))
}

struct ActiveConnection(prometheus::Gauge);

impl ActiveConnection {
    fn new(mountpoint: &str) -> Self {
        let gauge =
            crate::telemetry::metrics::UPSTREAM_CONNECTIONS.with_label_values(&[mountpoint]);
        gauge.inc();
        Self(gauge)
    }
}

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.0.dec();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::test_support::{config, wait_until, TestSource};

    #[tokio::test]
    async fn healthy_body_outlives_former_total_deadline() {
        let source = TestSource::start("audio/mpeg", Some(Duration::from_millis(50))).await;
        let (cfg, _, _) = config(&source.url);
        let client = build_http_client(&cfg).unwrap();
        let response = client.get(&source.url).send().await.unwrap();
        let mut body = response.bytes_stream();
        let started = tokio::time::Instant::now();
        // A one-second idle limit previously imposed a four-second lifetime.
        while started.elapsed() < Duration::from_millis(4500) {
            tokio::time::timeout(Duration::from_secs(2), body.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        assert_eq!(source.count(), 1);
    }

    #[tokio::test]
    async fn stalled_body_reconnects_and_cancellation_releases_connection() {
        let source = TestSource::start("audio/mpeg", None).await;
        let (cfg, mount, bc) = config(&source.url);
        let cancel = CancellationToken::new();
        let (tx, _rx) = mpsc::channel(8);
        let (meta, _metadata) = mpsc::channel(8);
        let (_, task) = spawn_ingest(
            cfg.clone(),
            build_http_client(&cfg).unwrap(),
            mount.clone(),
            bc,
            tx,
            meta,
            cancel.clone(),
        )
        .await
        .unwrap();
        wait_until(|| source.count() >= 2).await;
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            crate::telemetry::metrics::UPSTREAM_CONNECTIONS
                .with_label_values(&[&mount])
                .get(),
            0.0
        );
    }

    #[tokio::test]
    async fn immediate_eof_is_backed_off_and_backoff_is_cancellable() {
        let source = TestSource::start("audio/mpeg", Some(Duration::ZERO)).await;
        let (cfg, mount, bc) = config(&source.url);
        let cancel = CancellationToken::new();
        let (tx, _rx) = mpsc::channel(8);
        let (meta, _metadata) = mpsc::channel(8);
        let (_, task) = spawn_ingest(
            cfg.clone(),
            build_http_client(&cfg).unwrap(),
            mount,
            bc,
            tx,
            meta,
            cancel.clone(),
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(
            (2..=4).contains(&source.count()),
            "EOF retry storm: {}",
            source.count()
        );
        cancel.cancel();
        tokio::time::timeout(Duration::from_millis(200), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn headers_are_also_subject_to_inactivity_timeout() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });
        let (cfg, _, _) = config(&url);
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            build_http_client(&cfg).unwrap().get(url).send(),
        )
        .await
        .unwrap();
        assert!(result.unwrap_err().is_timeout());
        server.abort();
    }

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
