//! Pipeline creation, reuse and teardown.

use tokio_util::sync::CancellationToken;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use crate::error::{EngineError, Result};
use crate::ingest::{self, UpstreamInfo};
use crate::security::limits::{ListenerPermit, PipelinePermit};
use crate::transcoder::{EncodingProfile, StreamInput, TranscodedStream, Transcoder};
use crate::types::{Codec, Quality, StreamMetadata, StreamSpec};

use super::fanout::{FanEvent, Fanout, Subscriber};

/// What a handler needs after attaching to a pipeline: the subscription plus
/// the resolved output parameters (for response headers).
pub struct Attach {
    pub subscriber: Subscriber,
    pub content_type: String,
    pub meta_int: Option<u32>,
    pub bitrate_kbps: Option<u32>,
    pub metadata: Arc<RwLock<Option<StreamMetadata>>>,
    /// Held for the lifetime of the listener connection.
    pub _listener_permit: ListenerPermit,
    pub _pipeline_permit: Option<PipelinePermit>,
}

struct Entry {
    fanout: Fanout,
    metadata: Arc<RwLock<Option<StreamMetadata>>>,
    content_type: String,
    meta_int: Option<u32>,
    bitrate_kbps: Option<u32>,
    cancel: CancellationToken,
    is_transcode: bool,
    _permit: Option<PipelinePermit>,
    upstream_info: UpstreamInfo,
}

#[derive(Clone)]
pub struct PipelineManager {
    cfg: Arc<AppConfig>,
    client: reqwest::Client,
    transcoder: Arc<dyn Transcoder>,
    limits: Arc<crate::security::limits::LimitGuard>,
    entries: Arc<Mutex<HashMap<String, Entry>>>,
    shutting_down: Arc<std::sync::atomic::AtomicBool>,
    shutdown_cancel: CancellationToken,
}

impl PipelineManager {
    pub fn new(
        cfg: Arc<AppConfig>,
        client: reqwest::Client,
        transcoder: Arc<dyn Transcoder>,
        limits: Arc<crate::security::limits::LimitGuard>,
    ) -> Self {
        Self {
            cfg,
            client,
            transcoder,
            limits,
            entries: Arc::new(Mutex::new(HashMap::new())),
            shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            shutdown_cancel: CancellationToken::new(),
        }
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Snapshot of live pipeline keys for the status endpoint.
    pub async fn list_pipelines(&self) -> Vec<(String, u64, bool)> {
        let g = self.entries.lock().await;
        g.iter()
            .map(|(k, e)| (k.clone(), e.fanout.subscriber_count(), e.is_transcode))
            .collect()
    }

    pub async fn upstream_info(&self, key: &str) -> Option<UpstreamInfo> {
        self.entries
            .lock()
            .await
            .get(key)
            .map(|e| e.upstream_info.clone())
    }

    /// Resolve a request into a concrete [`StreamSpec`] *before* touching
    /// any resources. Errors map to 4xx — never starts upstream work.
    pub fn resolve_spec(
        &self,
        broadcast: &str,
        quality: Quality,
        codec: Codec,
    ) -> Result<(StreamSpec, Arc<crate::config::BroadcastConfig>)> {
        let bc = self
            .cfg
            .broadcast(broadcast)
            .ok_or(EngineError::BroadcastNotFound)?;
        if !bc.enabled {
            return Err(EngineError::BroadcastDisabled);
        }
        if !bc.quality_allowed(quality) {
            return Err(EngineError::BadParameter("quality not allowed".into()));
        }
        let codec = self.cfg.streaming.resolve_auto(codec);
        if !bc.codec_allowed(codec) {
            return Err(EngineError::BadParameter("codec not allowed".into()));
        }
        let bitrate = self.cfg.streaming.target_bitrate(quality, codec);
        // ORIGINAL relays the source unchanged when policy allows it and the
        // source codec is one we can copy verbatim (decided at attach time
        // from negotiated upstream info; config here only gates permission).
        let may_passthrough = quality == Quality::Original && bc.allow_passthrough;
        let spec = StreamSpec {
            broadcast: broadcast.to_string(),
            quality,
            codec,
            bitrate_bps: bitrate,
            passthrough: may_passthrough,
        };
        Ok((spec, Arc::new(bc.clone())))
    }

    /// Attach a listener (permit already reserved by preflight) to the
    /// shared pipeline for `spec`, creating it if needed.
    pub async fn attach(
        &self,
        spec: &StreamSpec,
        bc: Arc<crate::config::BroadcastConfig>,
        listener_permit: ListenerPermit,
        want_icy_meta: bool,
    ) -> Result<Attach> {
        let key = spec.key();
        let mut g = self.entries.lock().await;
        if self
            .shutting_down
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(EngineError::NotReady);
        }
        if let Some(e) = g.get_mut(&key) {
            let subscriber = e.fanout.subscribe();
            crate::telemetry::metrics::LISTENERS
                .with_label_values(&[&spec.broadcast])
                .inc();
            return Ok(Attach {
                subscriber,
                content_type: e.content_type.clone(),
                meta_int: if want_icy_meta { e.meta_int } else { None },
                bitrate_kbps: e.bitrate_kbps,
                metadata: e.metadata.clone(),
                _listener_permit: listener_permit,
                _pipeline_permit: None, // shared pipeline keeps its own permit
            });
        }

        // ---- create a fresh pipeline -------------------------------------
        // NB: everything below runs *without* holding the entries lock so a
        // slow upstream connect or encoder spawn cannot stall other
        // broadcasts. A rare duplicate creation under a race is harmless:
        // the second entry simply gets replaced by the reaper later.
        drop(g);

        let is_transcode = !spec.passthrough;
        let pipeline_permit = if is_transcode {
            Some(self.limits.clone().acquire_pipeline(true)?)
        } else {
            None
        };

        let (bytes_tx, bytes_rx) =
            mpsc::channel::<Vec<u8>>(self.cfg.limits.listener_queue_chunks.max(16));
        let (meta_tx, mut meta_rx) = mpsc::channel::<StreamMetadata>(16);
        let cancel = self.shutdown_cancel.child_token();
        // Includes client disconnect while attach is still awaiting startup.
        let startup_guard = cancel.clone().drop_guard();

        let (info, _ingest_join) = ingest::spawn_ingest(
            self.cfg.clone(),
            self.client.clone(),
            spec.broadcast.clone(),
            (*bc).clone(),
            bytes_tx,
            meta_tx,
            cancel.clone(),
        )
        .await?;

        crate::telemetry::metrics::UPSTREAM_CONNECTIONS
            .with_label_values(&[&spec.broadcast])
            .inc();

        let profile = EncodingProfile {
            codec: spec.codec,
            quality: spec.quality,
            bitrate_bps: spec.bitrate_bps.unwrap_or(0),
            sample_rate: 44100,
            channels: 2,
            threads: self.cfg.transcoding.threads_per_pipeline,
        };

        let mut encoded: TranscodedStream = match self
            .transcoder
            .start(StreamInput::new(bytes_rx), profile, cancel.clone())
            .await
        {
            Ok(s) => s,
            Err(e) => {
                cancel.cancel();
                crate::telemetry::metrics::TRANSCODER_FAILURES
                    .with_label_values(&[&spec.broadcast])
                    .inc();
                warn!(%key, error = %e, "transcoder failed to start");
                return Err(EngineError::UpstreamUnavailable);
            }
        };

        // Fail preflight before sending a successful HTTP response if both
        // backends fail to produce output. The router owns the retry budget.
        let first = tokio::select! {
            _ = cancel.cancelled() => None,
            result = tokio::time::timeout(std::time::Duration::from_secs(25), encoded.rx.recv()) => result.ok().flatten(),
        };
        let first = match first {
            Some(Ok(bytes)) => bytes,
            _ => {
                cancel.cancel();
                crate::telemetry::metrics::TRANSCODER_FAILURES
                    .with_label_values(&[&spec.broadcast])
                    .inc();
                return Err(EngineError::UpstreamUnavailable);
            }
        };

        let content_type = match spec.quality {
            Quality::Original => info
                .content_type
                .clone()
                .unwrap_or_else(|| spec.codec.content_type().to_string()),
            _ => spec.codec.content_type().to_string(),
        };
        let bitrate_kbps = match spec.quality {
            Quality::Original => info.bitrate_kbps,
            _ => spec.bitrate_bps.map(|b| (b / 1000) as u32),
        };
        // ICY interleaving is supported on MP3/AAC passthrough & mp3 output.
        let icy_capable = matches!(spec.codec, Codec::Mp3 | Codec::Aac)
            || (spec.passthrough && matches!(info.codec, Some(Codec::Mp3)));
        let meta_int = if want_icy_meta && icy_capable {
            Some(self.cfg.security.icy_metadata_interval_bytes)
        } else {
            None
        };

        let fanout = Fanout::new(
            self.cfg.limits.listener_queue_chunks,
            self.cfg.limits.listener_lag_timeout(),
        );
        let fanout = if is_transcode && spec.codec == Codec::Opus {
            fanout.with_ogg_headers()
        } else {
            fanout
        };
        let metadata = Arc::new(RwLock::new(None::<StreamMetadata>));

        // Subscribe before publishing container headers or fast startup output.
        let subscriber = fanout.subscribe();
        fanout.publish(FanEvent::Audio(Arc::new(first))).await;

        // Pump: encoded audio + live metadata -> fanout.
        let pump_fan = fanout.clone();
        let pump_key = key.clone();
        let pump_broadcast = spec.broadcast.clone();
        let pump_cancel = cancel.clone();
        let pump_meta = metadata.clone();
        tokio::spawn(async move {
            let mut rx = encoded.rx;
            let mut metadata_open = true;
            loop {
                tokio::select! {
                    ev = meta_rx.recv(), if metadata_open => {
                        if let Some(md) = ev {
                            *pump_meta.write().await = Some(md.clone());
                            pump_fan.publish(FanEvent::Metadata(Arc::new(md))).await;
                        } else { metadata_open = false; }
                    }
                    chunk = rx.recv() => {
                        match chunk {
                            Some(Ok(bytes)) => {
                                crate::telemetry::metrics::BYTES_SENT
                                    .with_label_values(&[&pump_broadcast])
                                    .inc_by(bytes.len() as f64);
                                pump_fan.publish(FanEvent::Audio(Arc::new(bytes))).await;
                            }
                            Some(Err(e)) => {
                                debug!(%pump_key, error = %e, "pipeline ended");
                                break;
                            }
                            None => break,
                        }
                    }
                    _ = pump_cancel.cancelled() => break,
                }
            }
            pump_fan.publish(FanEvent::End).await;
            pump_cancel.cancel();
        });
        let entry = Entry {
            fanout: fanout.clone(),
            metadata: metadata.clone(),
            content_type: content_type.clone(),
            meta_int,
            bitrate_kbps,
            cancel: cancel.clone(),
            is_transcode,
            _permit: pipeline_permit,
            upstream_info: info,
        };

        let mut g = self.entries.lock().await;
        if self.is_shutting_down() {
            return Err(EngineError::NotReady);
        }
        if let Some(existing) = g.get_mut(&key) {
            // A redundant session has no reaper: cancel it immediately.
            cancel.cancel();
            let sub = existing.fanout.subscribe();
            crate::telemetry::metrics::LISTENERS
                .with_label_values(&[&spec.broadcast])
                .inc();
            return Ok(Attach {
                subscriber: sub,
                content_type: existing.content_type.clone(),
                meta_int: if want_icy_meta {
                    existing.meta_int
                } else {
                    None
                },
                bitrate_kbps: existing.bitrate_kbps,
                metadata: existing.metadata.clone(),
                _listener_permit: listener_permit,
                _pipeline_permit: None,
            });
        }
        g.insert(key.clone(), entry);
        drop(g);

        crate::telemetry::metrics::PIPELINES_CREATED
            .with_label_values(&[
                &spec.broadcast,
                if is_transcode {
                    "transcode"
                } else {
                    "passthrough"
                },
            ])
            .inc();
        crate::telemetry::metrics::PIPELINES
            .with_label_values(&[
                &spec.broadcast,
                if is_transcode {
                    "transcode"
                } else {
                    "passthrough"
                },
            ])
            .inc();
        crate::telemetry::metrics::LISTENERS
            .with_label_values(&[&spec.broadcast])
            .inc();
        startup_guard.disarm();
        info!(%key, "pipeline created");

        // Reaper: detach pipeline after grace period without listeners.
        let this = self.clone();
        let reaper_key = key.clone();
        let grace = self.cfg.limits.pipeline_grace();
        let reaper_cancel = cancel.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = reaper_cancel.cancelled() => {
                        this.teardown(&reaper_key).await;
                        break;
                    }
                    _ = fanout.wait_empty() => {},
                }
                // grace window — if someone attaches meanwhile we loop again
                let woke = tokio::select! {
                    _ = tokio::time::sleep(grace) => true,
                    _ = reaper_cancel.cancelled() => false,
                };
                if !woke {
                    this.teardown(&reaper_key).await;
                    break;
                }
                if fanout.subscriber_count() == 0 {
                    this.teardown(&reaper_key).await;
                    break;
                }
            }
        });

        Ok(Attach {
            subscriber,
            content_type,
            meta_int,
            bitrate_kbps,
            metadata,
            _listener_permit: listener_permit,
            _pipeline_permit: None,
        })
    }

    async fn teardown(&self, key: &str) {
        let mut g = self.entries.lock().await;
        if let Some(e) = g.remove(key) {
            debug!(%key, "tearing down idle pipeline");
            e.cancel.cancel();
            let kind = if e.is_transcode {
                "transcode"
            } else {
                "passthrough"
            };
            crate::telemetry::metrics::PIPELINES
                .with_label_values(&[&key.split('|').next().unwrap_or(""), kind])
                .dec();
        }
    }

    /// Graceful shutdown: stop accepting new pipelines, tear down all.
    /// Decrement the listener gauge (called when a listener connection ends).
    pub fn listener_left(&self, broadcast: &str) {
        crate::telemetry::metrics::LISTENERS
            .with_label_values(&[broadcast])
            .dec();
    }

    pub async fn shutdown(&self) {
        self.shutting_down
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.shutdown_cancel.cancel();
        let mut g = self.entries.lock().await;
        for (_, e) in g.drain() {
            e.cancel.cancel();
        }
    }
}
