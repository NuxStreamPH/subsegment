//! One upstream download per broadcast for self-synchronizing MP3/ADTS AAC.
//! Each output owns a bounded input queue. On overflow we cancel that output
//! instead of losing compressed bytes or blocking unrelated encoders.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::{mpsc, watch, Mutex as AsyncMutex};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::config::{AppConfig, BroadcastConfig};
use crate::error::{EngineError, Result};
use crate::types::StreamMetadata;

use super::http::{spawn_ingest, UpstreamInfo};

pub struct SharedIngestPool {
    // Fixed at config load: arbitrary request keys cannot grow this registry.
    slots: HashMap<String, AsyncMutex<Weak<Session>>>,
    cfg: Arc<AppConfig>,
    client: reqwest::Client,
    shutdown: CancellationToken,
}

struct Branch {
    bytes: mpsc::Sender<Vec<u8>>,
    cancel: CancellationToken,
}

struct Session {
    info: UpstreamInfo,
    branches: Mutex<HashMap<u64, Branch>>,
    metadata: watch::Sender<StreamMetadata>,
    next_id: std::sync::atomic::AtomicU64,
    cancel: CancellationToken,
}

impl Drop for Session {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Retained by the output pipeline, including while its encoder starts.
pub struct IngestLease {
    session: Arc<Session>,
    id: u64,
}

impl Drop for IngestLease {
    fn drop(&mut self) {
        self.session.branches.lock().unwrap().remove(&self.id);
    }
}

pub struct IngestSubscription {
    pub info: UpstreamInfo,
    pub bytes: mpsc::Receiver<Vec<u8>>,
    pub metadata: watch::Receiver<StreamMetadata>,
    pub lease: IngestLease,
}

impl Session {
    fn subscribe(
        self: &Arc<Self>,
        capacity: usize,
        cancel: CancellationToken,
    ) -> IngestSubscription {
        let (tx, rx) = mpsc::channel(capacity);
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.branches
            .lock()
            .unwrap()
            .insert(id, Branch { bytes: tx, cancel });
        IngestSubscription {
            info: self.info.clone(),
            bytes: rx,
            metadata: self.metadata.subscribe(),
            lease: IngestLease {
                session: self.clone(),
                id,
            },
        }
    }

    fn publish(&self, mountpoint: &str, bytes: Vec<u8>) {
        self.branches.lock().unwrap().retain(|_, branch| {
            if branch.cancel.is_cancelled() {
                return false;
            }
            match branch.bytes.try_send(bytes.clone()) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    crate::telemetry::metrics::INGEST_BRANCH_DROPS
                        .with_label_values(&[mountpoint])
                        .inc();
                    warn!(%mountpoint, "encoder input queue full; cancelling slow output");
                    branch.cancel.cancel();
                    false
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    branch.cancel.cancel();
                    false
                }
            }
        });
    }
}

impl SharedIngestPool {
    pub fn new(cfg: Arc<AppConfig>, client: reqwest::Client, shutdown: CancellationToken) -> Self {
        Self {
            slots: cfg
                .broadcasts
                .keys()
                .map(|name| (name.clone(), AsyncMutex::new(Weak::new())))
                .collect(),
            cfg,
            client,
            shutdown,
        }
    }

    pub async fn subscribe(
        &self,
        mountpoint: &str,
        bc: &BroadcastConfig,
        output_cancel: CancellationToken,
    ) -> Result<IngestSubscription> {
        let slot = self
            .slots
            .get(mountpoint)
            .ok_or(EngineError::BroadcastNotFound)?;
        // Per-broadcast startup lock, never a global lock across network I/O.
        // Dropping the startup request drops this guard so another can retry.
        let mut cached = tokio::select! {
            _ = output_cancel.cancelled() => return Err(EngineError::UpstreamUnavailable),
            _ = self.shutdown.cancelled() => return Err(EngineError::NotReady),
            slot = slot.lock() => slot,
        };
        let capacity = self.cfg.limits.ingest_queue_chunks;
        if self.cfg.limits.share_upstream {
            if let Some(session) = cached.upgrade().filter(|s| !s.cancel.is_cancelled()) {
                return Ok(session.subscribe(capacity, output_cancel));
            }
        }

        let cancel = self.shutdown.child_token();
        let startup_guard = cancel.clone().drop_guard();
        // The staging queue is also bounded, independent of listener queues.
        let (bytes_tx, mut bytes_rx) = mpsc::channel(capacity);
        let (meta_tx, mut meta_rx) = mpsc::channel(16);
        let (info, ingest_task) = tokio::select! {
            _ = output_cancel.cancelled() => return Err(EngineError::UpstreamUnavailable),
            result = spawn_ingest(self.cfg.clone(), self.client.clone(), mountpoint.to_owned(),
                bc.clone(), bytes_tx, meta_tx, cancel.clone()) => result?,
        };
        let initial = StreamMetadata {
            station: bc.station_name.clone(),
            codec: info.content_type.clone(),
            content_type: info.content_type.clone(),
            bitrate: info.bitrate_kbps,
            ..Default::default()
        };
        let (metadata, _) = watch::channel(initial);
        let shareable = self.cfg.limits.share_upstream && can_join_live(&info);
        let session = Arc::new(Session {
            info,
            branches: Mutex::new(HashMap::new()),
            metadata,
            next_id: std::sync::atomic::AtomicU64::new(1),
            cancel: cancel.clone(),
        });
        // Register the first reader before consuming any source bytes.
        let subscription = session.subscribe(capacity, output_cancel);
        if shareable {
            *cached = Arc::downgrade(&session);
        }
        let weak = Arc::downgrade(&session);
        let mountpoint = mountpoint.to_owned();
        tokio::spawn(async move {
            // A task must never hold the session alive while waiting for I/O:
            // dropping the last output lease cancels the source immediately.
            let mut metadata_open = true;
            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    md = meta_rx.recv(), if metadata_open => {
                        match md {
                            Some(md) => {
                                let Some(session) = weak.upgrade() else { break };
                                session.metadata.send_modify(|current| *current = current.merge(&md));
                            }
                            None => metadata_open = false,
                        }
                    }
                    bytes = bytes_rx.recv() => {
                        let Some(bytes) = bytes else { break };
                        let Some(session) = weak.upgrade() else { break };
                        session.publish(&mountpoint, bytes);
                    }
                }
            }
            cancel.cancel();
            if let Some(session) = weak.upgrade() {
                for (_, branch) in session.branches.lock().unwrap().drain() {
                    branch.cancel.cancel();
                }
            }
            let _ = ingest_task.await;
        });
        startup_guard.disarm();
        Ok(subscription)
    }
}

fn can_join_live(info: &UpstreamInfo) -> bool {
    // Do not infer this from codec alone: Opus in Ogg and AAC in MP4 need
    // initialization headers. Unknown containers retain dedicated ingests.
    let mime = info
        .content_type
        .as_deref()
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    matches!(
        mime.as_str(),
        "audio/mpeg" | "audio/mp3" | "audio/aac" | "audio/aacp" | "audio/adts"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::{
        build_http_client,
        test_support::{config, wait_until, TestSource},
    };
    use std::time::Duration;

    #[tokio::test]
    async fn concurrent_subscribers_share_source_and_last_drop_stops_it() {
        let source = TestSource::start("audio/mpeg", Some(Duration::from_millis(10))).await;
        let (cfg, mount, bc) = config(&source.url);
        let pool = SharedIngestPool::new(
            cfg.clone(),
            build_http_client(&cfg).unwrap(),
            CancellationToken::new(),
        );
        let (a, b) = tokio::join!(
            pool.subscribe(&mount, &bc, CancellationToken::new()),
            pool.subscribe(&mount, &bc, CancellationToken::new()),
        );
        let (mut a, mut b) = (a.unwrap(), b.unwrap());
        assert_eq!(source.count(), 1);
        assert_eq!(a.bytes.recv().await, b.bytes.recv().await);
        let cancelled = a.lease.session.cancel.clone();
        drop(a);
        assert!(!cancelled.is_cancelled());
        assert!(b.bytes.recv().await.is_some());
        drop(b);
        tokio::time::timeout(Duration::from_secs(1), cancelled.cancelled())
            .await
            .unwrap();
        wait_until(|| {
            crate::telemetry::metrics::UPSTREAM_CONNECTIONS
                .with_label_values(&[&mount])
                .get()
                == 0.0
        })
        .await;
    }

    #[tokio::test]
    async fn late_subscriber_receives_merged_icy_metadata() {
        let mut bytes = vec![1, 2, 3, 4];
        let title = b"StreamTitle='Current song';";
        let mut block = title.to_vec();
        block.resize(32, 0);
        bytes.push(2);
        bytes.extend(block);
        let source =
            TestSource::with_bytes("audio/aac", Some(Duration::from_millis(10)), bytes, Some(4))
                .await;
        let (cfg, mount, bc) = config(&source.url);
        let pool = SharedIngestPool::new(
            cfg.clone(),
            build_http_client(&cfg).unwrap(),
            CancellationToken::new(),
        );
        let mut a = pool
            .subscribe(&mount, &bc, CancellationToken::new())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while a.metadata.borrow().title.as_deref() != Some("Current song") {
                a.metadata.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        let b = pool
            .subscribe(&mount, &bc, CancellationToken::new())
            .await
            .unwrap();
        let md = b.metadata.borrow().clone();
        assert_eq!(md.title.as_deref(), Some("Current song"));
        assert_eq!(md.station.as_deref(), Some("Test radio"));
        assert_eq!(md.bitrate, Some(128));
        assert_eq!(a.bytes.recv().await.unwrap(), [1, 2, 3, 4]);
        assert_eq!(source.count(), 1);
    }

    #[tokio::test]
    async fn header_dependent_containers_and_opt_out_keep_dedicated_sources() {
        for (mime, enabled) in [
            ("audio/ogg", true),
            ("audio/mp4", true),
            ("application/octet-stream", true),
            ("audio/mpeg", false),
        ] {
            let source = TestSource::start(mime, Some(Duration::from_millis(10))).await;
            let (mut cfg, mount, bc) = config(&source.url);
            Arc::make_mut(&mut cfg).limits.share_upstream = enabled;
            let pool = SharedIngestPool::new(
                cfg.clone(),
                build_http_client(&cfg).unwrap(),
                CancellationToken::new(),
            );
            let _a = pool
                .subscribe(&mount, &bc, CancellationToken::new())
                .await
                .unwrap();
            let _b = pool
                .subscribe(&mount, &bc, CancellationToken::new())
                .await
                .unwrap();
            assert_eq!(source.count(), 2, "{mime}, sharing={enabled}");
        }
    }

    #[tokio::test]
    async fn slow_branch_is_cancelled_without_losing_healthy_bytes() {
        let (metadata, _) = watch::channel(StreamMetadata::default());
        let session = Arc::new(Session {
            info: UpstreamInfo {
                content_type: Some("audio/mpeg".into()),
                codec: None,
                bitrate_kbps: None,
                icy_metaint: None,
            },
            branches: Mutex::new(HashMap::new()),
            metadata,
            next_id: std::sync::atomic::AtomicU64::new(1),
            cancel: CancellationToken::new(),
        });
        let slow_cancel = CancellationToken::new();
        let fast_cancel = CancellationToken::new();
        let slow = session.subscribe(2, slow_cancel.clone());
        let mut fast = session.subscribe(2, fast_cancel.clone());
        for i in 0..10u8 {
            session.publish("slow_branch_test", vec![i]);
            assert_eq!(fast.bytes.recv().await.unwrap(), [i]);
        }
        assert!(slow_cancel.is_cancelled());
        assert!(slow.bytes.is_closed());
        assert!(!fast_cancel.is_cancelled());
        assert_eq!(session.branches.lock().unwrap().len(), 1);
    }
}
