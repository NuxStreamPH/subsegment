//! Local streaming HTTP fixtures; no public radio/network dependency.
use axum::{body::Body, response::Response, routing::get, Router};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub(crate) struct TestSource {
    pub url: String,
    pub requests: Arc<AtomicUsize>,
    cancel: CancellationToken,
}

impl TestSource {
    /// None => accept headers but stall body; zero => immediate EOF.
    pub async fn start(mime: &'static str, interval: Option<Duration>) -> Self {
        Self::with_bytes(mime, interval, vec![0x55; 256], None).await
    }

    pub async fn with_bytes(
        mime: &'static str,
        interval: Option<Duration>,
        bytes: Vec<u8>,
        metaint: Option<u32>,
    ) -> Self {
        Self::with_chunks(mime, interval, vec![bytes], metaint).await
    }

    pub async fn with_chunks(
        mime: &'static str,
        interval: Option<Duration>,
        chunks: Vec<Vec<u8>>,
        metaint: Option<u32>,
    ) -> Self {
        let chunks = Arc::new(chunks);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/live", listener.local_addr().unwrap());
        let requests = Arc::new(AtomicUsize::new(0));
        let count = requests.clone();
        let cancel = CancellationToken::new();
        let shutdown = cancel.clone();
        let stream_cancel = cancel.clone();
        let app = Router::new().route("/live", get(move || {
            count.fetch_add(1, Ordering::SeqCst);
            let cancel = stream_cancel.clone();
            let chunks = chunks.clone();
            async move {
                let stream = futures_util::stream::unfold((cancel, chunks, 0usize), move |(cancel, chunks, i)| async move {
                    match interval {
                        Some(d) if d.is_zero() => None,
                        Some(d) => tokio::select! {
                            _ = cancel.cancelled() => None,
                            _ = tokio::time::sleep(d) => {
                                let bytes = chunks[i].clone();
                                let next = (i + 1) % chunks.len();
                                Some((Ok::<_, std::io::Error>(bytes), (cancel, chunks, next)))
                            },
                        },
                        None => { cancel.cancelled().await; None }
                    }
                });
                let mut response = Response::builder()
                    .header("content-type", mime).header("icy-br", "128");
                if let Some(n) = metaint { response = response.header("icy-metaint", n.to_string()); }
                response.body(Body::from_stream(stream)).unwrap()
            }
        }));
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .unwrap();
        });
        Self {
            url,
            requests,
            cancel,
        }
    }

    pub fn count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

impl Drop for TestSource {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

pub(crate) fn config(
    url: &str,
) -> (
    Arc<crate::config::AppConfig>,
    String,
    crate::config::BroadcastConfig,
) {
    let mount = format!("test_{}", uuid::Uuid::new_v4());
    let bc = crate::config::BroadcastConfig {
        enabled: true,
        source: crate::config::SourceConfig {
            r#type: crate::types::SourceType::Icecast,
            url: url.into(),
            headers: Default::default(),
        },
        station_name: Some("Test radio".into()),
        allowed_qualities: vec![],
        allowed_codecs: vec![],
        authentication_required: Some(false),
        tokens: vec![],
        allow_passthrough: true,
    };
    let mut cfg = crate::config::AppConfig::default();
    cfg.security.allow_private_upstream_addresses = true;
    cfg.security.require_authentication = false;
    cfg.security.allow_anonymous_streaming = true;
    cfg.limits.upstream_read_timeout_secs = 1;
    cfg.broadcasts.insert(mount.clone(), bc.clone());
    (Arc::new(cfg), mount, bc)
}

pub(crate) async fn wait_until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(4), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition did not become true");
}
