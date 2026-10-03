//! Pipeline creation, reuse and teardown.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex, Notify, RwLock};
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
    cancel: Arc<Notify>,
    is_transcode: bool,
    _permit: Option<PipelinePermit>,
    upstream_info: UpstreamInfo,
}

#[derive(Clone)]
pub struct PipelineManager {
    cfg: Arc<AppConfig>,
    client: reqwest::Client,
    transcoder: Arc<dyn Transcoder>,
    entries: Arc<Mutex<HashMap<String, Entry>>>,
    shutting_down: Arc<std::sync::atomic::AtomicBool>,
}

impl PipelineManager {
    pub fn new(
        cfg: Arc<AppConfig>,
        client: reqwest::Client,
        transcoder: Arc<dyn Transcoder>,
    ) -> Self {
        Self {
            cfg,
            client,
            transcoder,
            entries: Arc::new(Mutex::new(HashMap::new())),
            shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Snapshot of live pipeline keys for the status endpoint.
    pub async fn list_pipelines(&self) -> Vec<(String, u64, bool)> {
        let g = self.entries.lock().await;
        g.iter()
            .map(|(k, e)| (k.clone(), e.fanout.subscriber_count(), e.is_transcode))
            .collect()
    }

    pub async fn upstream_info(&self, key: &str) -> Option<UpstreamInfo> {
        self.entries.lock().await.get(key).map(|e| e.upstream_info.clone())
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
        if self.shutting_down.load(std::sync::atomic::Ordering::Relaxed) {
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
        let is_transcode = !spec.passthrough;
        let pipeline_permit = if is_transcode {
            Some(listener_permit.guard.clone().acquire_pipeline(true)?)
        } else {
            None
        };

        let (bytes_tx, bytes_rx) = mpsc::channel::<Vec<u8>>(
            self.cfg.limits.listener_queue_chunks.max(16),
        );
        let (meta_tx, mut meta_rx) = mpsc::channel::<StreamMetadata>(16);
        let cancel = Arc::new(Notify::new());

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

        let encoded: TranscodedStream = if spec.passthrough {
            // ORIGINAL: copy bytes unchanged; codec/content-type from source.
            self.transcoder
                .start(StreamInput::new(bytes_rx), profile.clone(), cancel.clone())
                .await
                .unwrap_or_else(|_| {
                    // passthrough backend should not fail; fall back safely
                    unreachable!("passthrough start cannot fail")
                })
        } else {
            match self
                .transcoder
                .start(StreamInput::new(bytes_rx), profile.clone(), cancel.clone())
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    cancel.notify_waiters();
                    warn!(%key, error = %e, "transcoder failed to start");
                    return Err(EngineError::UpstreamUnavailable);
                }
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
        let metadata = Arc::new(RwLock::new::<Option<StreamMetadata>>(None));

        // Pump: encoded audio + live metadata -> fanout.
        let pump_fan = fanout.clone();
        let pump_key = key.clone();
        let pump_broadcast = spec.broadcast.clone();
        tokio::spawn(async move {
            let mut rx = encoded.rx;
            loop {
                tokio::select! {
                    ev = meta_rx.recv() => {
                        if let Some(md) = ev {
                            *metadata.write().await = Some(md.clone());
                            pump_fan.publish(FanEvent::Metadata(Arc::new(md))).await;
                        }
                    }
                    chunk = rx.recv() => {
                        match chunk {
                            Some(Ok(bytes)) => {
                                crate::telemetry::metrics::BYTES_SENT
                                    .with_label_values(&[&pump_broadcast])
                                    .inc_by(bytes.len() as u64);
                                pump_fan.publish(FanEvent::Audio(Arc::new(bytes))).await;
                            }
                            Some(Err(e)) => {
                                debug!(%pump_key, error = %e, "pipeline ended");
                                break;
                            }
                            None => break,
                        }
                    }
                    _ = cancel.notified() => break,
                }
            }
            pump_fan.publish(FanEvent::End).await;
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
        let subscriber = fanout.subscribe();
        g.insert(key.clone(), entry);
        drop(g);

        crate::telemetry::metrics::PIPELINES_CREATED
            .with_label_values(&[&spec.broadcast, if is_transcode { "transcode" } else { "passthrough" }])
            .inc();
        crate::telemetry::metrics::PIPELINES
            .with_label_values(&[&spec.broadcast, if is_transcode { "transcode" } else { "passthrough" }])
            .inc();
        crate::telemetry::metrics::LISTENERS
            .with_label_values(&[&spec.broadcast])
            .inc();
        info!(%key, "pipeline created");

        // Reaper: detach pipeline after grace period without listeners.
        let this = self.clone();
        let reaper_key = key.clone();
        let grace = self.cfg.limits.pipeline_grace();
        let reaper_cancel = cancel.clone();
        tokio::spawn(async move {
            loop {
                fanout.wait_empty().await;
                // grace window — if someone attaches meanwhile we loop again
                let woke = tokio::select! {
                    _ = tokio::time::sleep(grace) => true,
                    _ = reaper_cancel.notified() => false,
                };
                if !woke {
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
            e.cancel.notify_waiters();
            let kind = if e.is_transcode { "transcode" } else { "passthrough" };
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
        let mut g = self.entries.lock().await;
        for (_, e) in g.drain() {
            e.cancel.notify_waiters();
        }
    }
}
