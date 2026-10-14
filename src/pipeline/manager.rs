//! Pipeline creation, reuse and teardown.

use tokio_util::sync::CancellationToken;

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use crate::error::{EngineError, Result};
use crate::ingest::shared::{IngestLease, IngestSubscription, SharedIngestPool};
use crate::ingest::UpstreamInfo;
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
    generation: uuid::Uuid,
    _ingest: IngestLease,
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
    ingests: Arc<SharedIngestPool>,
    transcoder: Arc<dyn Transcoder>,
    limits: Arc<crate::security::limits::LimitGuard>,
    entries: Arc<Mutex<HashMap<String, Entry>>>,
    startup_locks: Arc<Mutex<HashMap<String, Weak<Mutex<()>>>>>,
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
        let shutdown_cancel = CancellationToken::new();
        let ingests = Arc::new(SharedIngestPool::new(
            cfg.clone(),
            client,
            shutdown_cancel.clone(),
        ));
        Self {
            cfg,
            ingests,
            transcoder,
            limits,
            entries: Arc::new(Mutex::new(HashMap::new())),
            startup_locks: Arc::new(Mutex::new(HashMap::new())),
            shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            shutdown_cancel,
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
        // Bounds initial upstream retries as well as waiting behind another
        // startup. Timeout drops all startup guards and releases permits.
        tokio::select! {
            _ = self.shutdown_cancel.cancelled() => Err(EngineError::NotReady),
            result = tokio::time::timeout(
                std::time::Duration::from_secs(self.cfg.limits.pipeline_startup_timeout_secs),
                self.attach_inner(spec, bc, listener_permit, want_icy_meta),
            ) => result.unwrap_or(Err(EngineError::UpstreamUnavailable)),
        }
    }

    async fn attach_inner(
        &self,
        spec: &StreamSpec,
        bc: Arc<crate::config::BroadcastConfig>,
        listener_permit: ListenerPermit,
        want_icy_meta: bool,
    ) -> Result<Attach> {
        let key = spec.key();
        let gate = {
            let mut locks = self.startup_locks.lock().await;
            locks.retain(|_, lock| lock.strong_count() > 0);
            let slot = locks.entry(key.clone()).or_default();
            match slot.upgrade() {
                Some(gate) => gate,
                None => {
                    let gate = Arc::new(Mutex::new(()));
                    *slot = Arc::downgrade(&gate);
                    gate
                }
            }
        };
        // Coalesce identical starts before reserving encoder capacity.
        let _startup_lock = gate.lock().await;
        let mut g = self.entries.lock().await;
        if self
            .shutting_down
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(EngineError::NotReady);
        }
        if g.get(&key).is_some_and(|e| e.cancel.is_cancelled()) {
            if let Some(e) = g.remove(&key) {
                Self::release_entry(&key, e);
            }
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
        // Different output keys can start concurrently without a global
        // entries lock spanning upstream connections or encoder startup.
        drop(g);

        let is_transcode = !spec.passthrough;
        let pipeline_permit = if is_transcode {
            Some(self.limits.clone().acquire_pipeline(true)?)
        } else {
            None
        };

        let cancel = self.shutdown_cancel.child_token();
        // Includes client disconnect while attach is still awaiting startup.
        let startup_guard = cancel.clone().drop_guard();

        let IngestSubscription {
            info,
            bytes: bytes_rx,
            metadata: mut meta_rx,
            lease,
        } = self
            .ingests
            .subscribe(&spec.broadcast, &bc, cancel.clone())
            .await?;

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
        let meta_int = if icy_capable {
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
        let metadata = Arc::new(RwLock::new(Some(meta_rx.borrow_and_update().clone())));

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
                    ev = meta_rx.changed(), if metadata_open => {
                        if ev.is_ok() {
                            let md = meta_rx.borrow_and_update().clone();
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
        let generation = uuid::Uuid::new_v4();
        let entry = Entry {
            generation,
            _ingest: lease,
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
        if cancel.is_cancelled() {
            return Err(EngineError::UpstreamUnavailable);
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
                        this.teardown(&reaper_key, generation, false).await;
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
                    this.teardown(&reaper_key, generation, false).await;
                    break;
                }
                if this.teardown(&reaper_key, generation, true).await {
                    break;
                }
            }
        });

        Ok(Attach {
            subscriber,
            content_type,
            meta_int: if want_icy_meta { meta_int } else { None },
            bitrate_kbps,
            metadata,
            _listener_permit: listener_permit,
            _pipeline_permit: None,
        })
    }

    async fn teardown(&self, key: &str, generation: uuid::Uuid, only_if_idle: bool) -> bool {
        let mut g = self.entries.lock().await;
        let Some(entry) = g.get(key) else { return true };
        // An old reaper must not remove a replacement with the same key.
        if entry.generation != generation {
            return true;
        }
        // Check under the same lock as attach/subscribe to avoid idle races.
        if only_if_idle && entry.fanout.subscriber_count() != 0 {
            return false;
        }
        if let Some(e) = g.remove(key) {
            Self::release_entry(key, e);
        }
        true
    }

    fn release_entry(key: &str, e: Entry) {
        debug!(%key, "tearing down pipeline");
        e.cancel.cancel();
        let kind = if e.is_transcode {
            "transcode"
        } else {
            "passthrough"
        };
        crate::telemetry::metrics::PIPELINES
            .with_label_values(&[key.split('|').next().unwrap_or(""), kind])
            .dec();
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
        for (key, e) in g.drain() {
            Self::release_entry(&key, e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::{
        build_http_client,
        test_support::{config, wait_until, TestSource},
    };
    use crate::security::limits::LimitGuard;
    use crate::transcoder::{PassthroughTranscoder, TranscodeError};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    struct CountingTranscoder(Arc<AtomicUsize>);

    #[async_trait::async_trait]
    impl Transcoder for CountingTranscoder {
        async fn start(
            &self,
            input: StreamInput,
            profile: EncodingProfile,
            cancel: CancellationToken,
        ) -> std::result::Result<TranscodedStream, TranscodeError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            // Make overlapping startup requests deterministic.
            tokio::time::sleep(Duration::from_millis(50)).await;
            PassthroughTranscoder::new()
                .start(input, profile, cancel)
                .await
        }
        fn name(&self) -> &'static str {
            "counting"
        }
    }

    fn manager(cfg: Arc<AppConfig>) -> (PipelineManager, Arc<LimitGuard>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let limits = Arc::new(LimitGuard::new(&cfg));
        let manager = PipelineManager::new(
            cfg.clone(),
            build_http_client(&cfg).unwrap(),
            Arc::new(CountingTranscoder(calls.clone())),
            limits.clone(),
        );
        (manager, limits, calls)
    }

    // Exercise decoder synchronization when a new profile joins mid-frame,
    // rather than merely checking that mock byte queues received data.
    async fn media_late_join(backend: Arc<dyn Transcoder>) {
        for (mime, encoder, format) in [
            ("audio/mpeg", "libmp3lame", "mp3"),
            ("audio/aac", "aac", "adts"),
        ] {
            let mut command = tokio::process::Command::new("ffmpeg");
            command.args([
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:duration=3",
                "-c:a",
                encoder,
                "-b:a",
                "128k",
            ]);
            if format == "mp3" {
                command.args(["-write_xing", "0", "-id3v2_version", "0"]);
            }
            let generated = command
                .args(["-f", format, "pipe:1"])
                .output()
                .await
                .expect("install ffmpeg for media tests");
            assert!(
                generated.status.success(),
                "{}",
                String::from_utf8_lossy(&generated.stderr)
            );
            let chunks = generated.stdout.chunks(2048).map(|c| c.to_vec()).collect();
            let source =
                TestSource::with_chunks(mime, Some(Duration::from_millis(40)), chunks, None).await;
            let (cfg, mount, _) = config(&source.url);
            let limits = Arc::new(LimitGuard::new(&cfg));
            let manager = PipelineManager::new(
                cfg.clone(),
                build_http_client(&cfg).unwrap(),
                backend.clone(),
                limits.clone(),
            );
            let mut tasks = Vec::new();
            let mut outputs = Vec::new();
            for quality in [Quality::Low, Quality::High] {
                let (spec, bc) = manager.resolve_spec(&mount, quality, Codec::Opus).unwrap();
                let attached = tokio::time::timeout(
                    Duration::from_secs(15),
                    manager.attach(&spec, bc, limits.acquire_listener(&mount).unwrap(), false),
                )
                .await
                .unwrap()
                .unwrap();
                let (ready, output) = tokio::sync::oneshot::channel();
                tasks.push(tokio::spawn(async move {
                    let _permit = attached._listener_permit;
                    let mut receiver = attached.subscriber.into_receiver();
                    let mut ready = Some(ready);
                    let mut bytes = Vec::new();
                    while let Some(event) = receiver.recv().await {
                        if let FanEvent::Audio(chunk) = event {
                            if ready.is_some() {
                                bytes.extend_from_slice(&chunk);
                                if bytes.len() >= 8192 {
                                    let _ = ready.take().unwrap().send(std::mem::take(&mut bytes));
                                }
                            }
                        }
                    }
                }));
                outputs.push(
                    tokio::time::timeout(Duration::from_secs(15), output)
                        .await
                        .unwrap()
                        .unwrap(),
                );
                // The next profile must join after media has already flowed.
            }
            assert_eq!(source.count(), 1, "{mime} used duplicate downloads");
            manager.shutdown().await;
            for task in tasks {
                tokio::time::timeout(Duration::from_secs(2), task)
                    .await
                    .unwrap()
                    .unwrap();
            }
            for output in outputs {
                let file = tempfile::NamedTempFile::new().unwrap();
                std::fs::write(file.path(), output).unwrap();
                let decoded = tokio::process::Command::new("ffmpeg")
                    .args(["-v", "error", "-i"])
                    .arg(file.path())
                    .args(["-f", "s16le", "pipe:1"])
                    .output()
                    .await
                    .unwrap();
                assert!(
                    decoded.status.success(),
                    "{}",
                    String::from_utf8_lossy(&decoded.stderr)
                );
                assert!(decoded.stdout.len() > 1000, "no decoded PCM from {mime}");
            }
        }
    }

    #[tokio::test]
    async fn ffmpeg_decodes_late_joining_shared_mp3_and_aac() {
        media_late_join(Arc::new(crate::transcoder::FfmpegTranscoder::new(
            Arc::new(crate::config::TranscodingConfig::default()),
        )))
        .await;
    }

    #[cfg(feature = "gstreamer")]
    #[tokio::test]
    async fn gstreamer_decodes_late_joining_shared_mp3_and_aac() {
        media_late_join(Arc::new(
            crate::transcoder::gstreamer::GstreamerTranscoder::new(Arc::new(
                crate::config::TranscodingConfig::default(),
            )),
        ))
        .await;
    }

    #[tokio::test]
    async fn identical_starts_coalesce_before_encoder_permit_and_keep_icy_opt_in() {
        let source = TestSource::start("audio/mpeg", Some(Duration::from_millis(10))).await;
        let (mut cfg, mount, _) = config(&source.url);
        Arc::make_mut(&mut cfg).limits.max_transcoding_pipelines = 1;
        let (manager, limits, calls) = manager(cfg);
        let (spec, bc) = manager
            .resolve_spec(&mount, Quality::Medium, Codec::Mp3)
            .unwrap();
        let starts = (0..12).map(|i| {
            manager.attach(
                &spec,
                bc.clone(),
                limits.acquire_listener(&mount).unwrap(),
                i != 0,
            )
        });
        let attached = futures::future::join_all(starts)
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect::<Vec<_>>();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(source.count(), 1);
        assert_eq!(limits.active_pipelines(), 1);
        assert_eq!(attached[0].meta_int, None);
        assert!(attached[1].meta_int.is_some());
        manager.shutdown().await;
        drop(attached);
        assert_eq!(limits.active_listeners(), 0);
        assert_eq!(limits.active_pipelines(), 0);
        wait_until(|| {
            crate::telemetry::metrics::UPSTREAM_CONNECTIONS
                .with_label_values(&[&mount])
                .get()
                == 0.0
        })
        .await;
    }

    #[tokio::test]
    async fn different_profiles_share_source_and_old_reaper_cannot_remove_replacement() {
        let source = TestSource::start("audio/mpeg", Some(Duration::from_millis(10))).await;
        let (cfg, mount, _) = config(&source.url);
        let (manager, limits, calls) = manager(cfg);
        let (low, bc) = manager
            .resolve_spec(&mount, Quality::Low, Codec::Mp3)
            .unwrap();
        let (high, _) = manager
            .resolve_spec(&mount, Quality::High, Codec::Mp3)
            .unwrap();
        let (a, b) = tokio::join!(
            manager.attach(
                &low,
                bc.clone(),
                limits.acquire_listener(&mount).unwrap(),
                false
            ),
            manager.attach(
                &high,
                bc.clone(),
                limits.acquire_listener(&mount).unwrap(),
                false
            ),
        );
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(source.count(), 1);
        let old = manager
            .entries
            .lock()
            .await
            .get(&low.key())
            .unwrap()
            .generation;
        assert!(
            !manager.teardown(&low.key(), old, true).await,
            "active listener must prevent idle teardown"
        );
        manager.teardown(&low.key(), old, false).await;
        drop(a);
        let c = manager
            .attach(&low, bc, limits.acquire_listener(&mount).unwrap(), false)
            .await
            .unwrap();
        manager.teardown(&low.key(), old, false).await;
        assert_eq!(manager.list_pipelines().await.len(), 2);
        assert_eq!(source.count(), 1);
        manager.shutdown().await;
        drop((b, c));
        assert_eq!(limits.active_pipelines(), 0);
    }

    #[tokio::test]
    async fn failed_startup_has_a_deadline_and_releases_every_permit() {
        let source = TestSource::start("audio/mpeg", None).await;
        let (mut cfg, mount, _) = config(&source.url);
        Arc::make_mut(&mut cfg).limits.pipeline_startup_timeout_secs = 1;
        let (manager, limits, _) = manager(cfg);
        let (spec, bc) = manager
            .resolve_spec(&mount, Quality::Medium, Codec::Mp3)
            .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            manager.attach(&spec, bc, limits.acquire_listener(&mount).unwrap(), false),
        )
        .await
        .unwrap();
        assert!(result.is_err());
        assert_eq!(limits.active_listeners(), 0);
        assert_eq!(limits.active_pipelines(), 0);
        assert!(manager.list_pipelines().await.is_empty());
        wait_until(|| {
            crate::telemetry::metrics::UPSTREAM_CONNECTIONS
                .with_label_values(&[&mount])
                .get()
                == 0.0
        })
        .await;
    }

    #[tokio::test]
    async fn abandoned_startup_does_not_block_other_stations_or_leak_source() {
        let stalled = TestSource::start("audio/mpeg", None).await;
        let healthy = TestSource::start("audio/mpeg", Some(Duration::from_millis(10))).await;
        let (mut cfg, mount, bc) = config(&stalled.url);
        let other = format!("other_{mount}");
        let mut other_bc = bc;
        other_bc.source.url = healthy.url.clone();
        Arc::make_mut(&mut cfg)
            .broadcasts
            .insert(other.clone(), other_bc);
        let (manager, limits, _) = manager(cfg);
        let m = manager.clone();
        let permit = limits.acquire_listener(&mount).unwrap();
        let (spec, bc) = manager
            .resolve_spec(&mount, Quality::Low, Codec::Mp3)
            .unwrap();
        let task = tokio::spawn(async move { m.attach(&spec, bc, permit, false).await });
        wait_until(|| stalled.count() == 1).await;
        let (spec, bc) = manager
            .resolve_spec(&other, Quality::Low, Codec::Mp3)
            .unwrap();
        let attached = tokio::time::timeout(
            Duration::from_secs(2),
            manager.attach(&spec, bc, limits.acquire_listener(&other).unwrap(), false),
        )
        .await
        .unwrap()
        .unwrap();
        task.abort();
        let _ = task.await;
        assert_eq!(limits.listeners_for(&mount), 0);
        wait_until(|| {
            crate::telemetry::metrics::UPSTREAM_CONNECTIONS
                .with_label_values(&[&mount])
                .get()
                == 0.0
        })
        .await;
        manager.shutdown().await;
        drop(attached);
        assert_eq!(limits.active_pipelines(), 0);
    }
}
