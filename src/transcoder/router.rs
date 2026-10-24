//! Prefer GStreamer and retry with FFmpeg before the first output byte.
//! The startup prefix is bounded, retained verbatim and replayed exactly once.
//! After output is committed, errors end the stream: never splice containers.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::{EncodingProfile, StreamInput, TranscodeError, TranscodedStream, Transcoder};
use crate::types::Quality;

const REPLAY_LIMIT: usize = 1024 * 1024;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

pub struct BackendRouter {
    primary: Option<Arc<dyn Transcoder>>,
    fallback: Option<Arc<dyn Transcoder>>,
    gst_qualities: Vec<Quality>,
}

impl BackendRouter {
    pub fn new(
        primary: Option<Arc<dyn Transcoder>>,
        fallback: Option<Arc<dyn Transcoder>>,
        gst_qualities: Vec<Quality>,
    ) -> Self {
        Self {
            primary,
            fallback,
            gst_qualities,
        }
    }

    fn wants_primary(&self, profile: &EncodingProfile) -> bool {
        self.primary.is_some()
            && (self.gst_qualities.is_empty() || self.gst_qualities.contains(&profile.quality))
    }

    pub fn backend_name_for(&self, profile: &EncodingProfile) -> &'static str {
        if profile.quality == Quality::Original || profile.bitrate_bps == 0 {
            "passthrough"
        } else if self.wants_primary(profile) {
            self.primary.as_ref().unwrap().name()
        } else {
            self.fallback.as_ref().map_or("none", |b| b.name())
        }
    }
}

#[async_trait]
impl Transcoder for BackendRouter {
    async fn start(
        &self,
        input: StreamInput,
        profile: EncodingProfile,
        cancel: CancellationToken,
    ) -> Result<TranscodedStream, TranscodeError> {
        if profile.quality == Quality::Original || profile.bitrate_bps == 0 {
            return super::PassthroughTranscoder::new()
                .start(input, profile, cancel)
                .await;
        }
        let primary = if self.wants_primary(&profile) {
            self.primary.clone()
        } else {
            None
        };
        let mut backends: Vec<Arc<dyn Transcoder>> = primary.into_iter().collect();
        if let Some(fallback) = &self.fallback {
            backends.push(fallback.clone());
        }
        if backends.is_empty() {
            return Err(TranscodeError::Unavailable);
        }
        let (tx, rx) = mpsc::channel(8);
        tokio::spawn(async move {
            let result = relay(input.into_receiver(), profile, &backends, &tx, &cancel).await;
            if let Err(e) = result {
                tokio::select! {
                    _ = cancel.cancelled() => {},
                    _ = tx.send(Err(e)) => {},
                }
            }
        });
        Ok(TranscodedStream::from_receiver(rx))
    }
    fn name(&self) -> &'static str {
        "router"
    }
}

async fn relay(
    mut input: mpsc::Receiver<Vec<u8>>,
    profile: EncodingProfile,
    backends: &[Arc<dyn Transcoder>],
    output: &mpsc::Sender<Result<Vec<u8>, TranscodeError>>,
    cancel: &CancellationToken,
) -> Result<(), TranscodeError> {
    let mut replay = VecDeque::<Vec<u8>>::new();
    let mut eof = false;
    let mut last_error = TranscodeError::Unavailable;
    // An over-budget chunk is never copied into replay or given to the failed
    // backend. It is passed after the retained prefix to the fallback.
    let mut overflow = None;
    for (index, backend) in backends.iter().enumerate() {
        let attempt = cancel.child_token();
        let _cancel_on_exit = attempt.clone().drop_guard();
        let (feed, feed_rx) = mpsc::channel(4);
        let start = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            _ = output.closed() => return Ok(()),
            result = tokio::time::timeout(STARTUP_TIMEOUT, backend.start(StreamInput::new(feed_rx), profile.clone(), attempt.clone())) => result,
        };
        let mut encoded = match start {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                last_error = e;
                continue;
            }
            Err(_) => {
                last_error = TranscodeError::ProcessFailed;
                continue;
            }
        };
        let retryable = index + 1 < backends.len();
        let mut pending = replay.clone();
        if let Some(bytes) = overflow.take() {
            pending.push_back(bytes);
        }
        let mut retained: usize = replay.iter().map(Vec::len).sum();
        let mut committed = false;
        let mut feed = Some(feed);
        let deadline = tokio::time::sleep(STARTUP_TIMEOUT);
        tokio::pin!(deadline);
        loop {
            if eof && pending.is_empty() {
                feed.take();
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(()),
                _ = output.closed() => return Ok(()),
                _ = &mut deadline, if !committed => {
                    last_error = TranscodeError::ProcessFailed;
                    break;
                }
                item = encoded.rx.recv() => match item {
                    Some(Ok(bytes)) if bytes.is_empty() => {},
                    Some(Ok(bytes)) => {
                        committed = true;
                        replay.clear();
                        tokio::select! {
                            _ = cancel.cancelled() => return Ok(()),
                            sent = output.send(Ok(bytes)) => if sent.is_err() { return Ok(()); },
                        }
                    }
                    Some(Err(e)) => {
                        if committed { return Err(e); }
                        last_error = e;
                        break;
                    }
                    None => {
                        if committed { return Ok(()); }
                        last_error = TranscodeError::ProcessFailed;
                        break;
                    }
                },
                permit = async { feed.as_ref().unwrap().clone().reserve_owned().await }, if feed.is_some() && !pending.is_empty() => {
                    match permit {
                        Ok(permit) => { permit.send(pending.pop_front().unwrap()); }
                        // Drain the output/error; input closure can mean normal EOF.
                        Err(_) => { feed.take(); }
                    }
                }
                chunk = input.recv(), if !eof && pending.is_empty() && feed.is_some() => {
                    match chunk {
                        Some(bytes) if bytes.is_empty() => {},
                        Some(bytes) => {
                            if !committed && retryable {
                                if bytes.len() > REPLAY_LIMIT.saturating_sub(retained) {
                                    overflow = Some(bytes);
                                    last_error = TranscodeError::ProcessFailed;
                                    break;
                                }
                                retained += bytes.len();
                                replay.push_back(bytes.clone());
                            }
                            pending.push_back(bytes);
                        }
                        None => eof = true,
                    }
                }
            }
        }
        warn!(backend = backend.name(), error = %last_error, retry = retryable, "transcoder startup failed");
    }
    Err(last_error)
}

pub fn parse_gst_qualities(items: &[String]) -> Vec<Quality> {
    items
        .iter()
        .filter_map(|s| s.parse::<Quality>().ok())
        .filter(|q| *q != Quality::Original)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Codec;
    use std::sync::atomic::{AtomicUsize, Ordering};

    enum Behavior {
        Unavailable,
        FailAfterInput,
        OutputThenFail,
        Echo,
        Stall,
    }
    struct Fake {
        behavior: Behavior,
        calls: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl Transcoder for Fake {
        async fn start(
            &self,
            input: StreamInput,
            _: EncodingProfile,
            cancel: CancellationToken,
        ) -> Result<TranscodedStream, TranscodeError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if matches!(self.behavior, Behavior::Unavailable) {
                return Err(TranscodeError::Unavailable);
            }
            let mut input = input.into_receiver();
            let (tx, rx) = mpsc::channel(4);
            let fail = matches!(self.behavior, Behavior::FailAfterInput);
            let after = matches!(self.behavior, Behavior::OutputThenFail);
            let stall = matches!(self.behavior, Behavior::Stall);
            tokio::spawn(async move {
                if stall {
                    cancel.cancelled().await;
                    return;
                }
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        bytes = input.recv() => match bytes {
                            Some(bytes) => {
                                if fail { let _ = tx.send(Err(TranscodeError::ProcessFailed)).await; break; }
                                if tx.send(Ok(bytes)).await.is_err() { break; }
                                if after { let _ = tx.send(Err(TranscodeError::ProcessFailed)).await; break; }
                            }
                            None => break,
                        }
                    }
                }
            });
            Ok(TranscodedStream::from_receiver(rx))
        }
        fn name(&self) -> &'static str {
            "fake"
        }
    }
    fn fake(behavior: Behavior) -> (Arc<dyn Transcoder>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(Fake {
                behavior,
                calls: calls.clone(),
            }),
            calls,
        )
    }
    async fn run(behavior: Behavior) -> (Vec<u8>, bool, usize) {
        let (primary, _) = fake(behavior);
        let (fallback, calls) = fake(Behavior::Echo);
        let router = BackendRouter::new(Some(primary), Some(fallback), vec![]);
        let (tx, rx) = mpsc::channel(4);
        for bytes in [
            b"container header".to_vec(),
            b"audio one".to_vec(),
            b"audio two".to_vec(),
        ] {
            tx.send(bytes).await.unwrap();
        }
        drop(tx);
        let mut out = router
            .start(
                StreamInput::new(rx),
                EncodingProfile::new(Codec::Mp3, 64000),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let mut bytes = Vec::new();
        let mut failed = false;
        while let Some(item) = tokio::time::timeout(Duration::from_secs(2), out.rx.recv())
            .await
            .unwrap()
        {
            match item {
                Ok(b) => bytes.extend(b),
                Err(_) => failed = true,
            }
        }
        (bytes, failed, calls.load(Ordering::SeqCst))
    }
    #[tokio::test]
    async fn unavailable_retries_without_losing_input() {
        let (bytes, failed, calls) = run(Behavior::Unavailable).await;
        assert_eq!(bytes, b"container headeraudio oneaudio two");
        assert!(!failed);
        assert_eq!(calls, 1);
    }
    #[tokio::test]
    async fn decoder_failure_replays_consumed_prefix_exactly_once() {
        let (bytes, failed, calls) = run(Behavior::FailAfterInput).await;
        assert_eq!(bytes, b"container headeraudio oneaudio two");
        assert!(!failed);
        assert_eq!(calls, 1);
    }
    #[tokio::test]
    async fn never_switches_backend_after_output() {
        let (bytes, failed, calls) = run(Behavior::OutputThenFail).await;
        assert_eq!(bytes, b"container header");
        assert!(failed);
        assert_eq!(calls, 0);
    }
    #[tokio::test(start_paused = true)]
    async fn startup_timeout_falls_back() {
        let (primary, _) = fake(Behavior::Stall);
        let (fallback, calls) = fake(Behavior::Echo);
        let router = BackendRouter::new(Some(primary), Some(fallback), vec![]);
        let (tx, rx) = mpsc::channel(1);
        tx.send(vec![42]).await.unwrap();
        drop(tx);
        let mut out = router
            .start(
                StreamInput::new(rx),
                EncodingProfile::new(Codec::Mp3, 64000),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(out.rx.recv().await.unwrap().unwrap(), vec![42]);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn replay_budget_switches_without_dropping_overflow_chunk() {
        let (primary, _) = fake(Behavior::Stall);
        let (fallback, _) = fake(Behavior::Echo);
        let router = BackendRouter::new(Some(primary), Some(fallback), vec![]);
        let (tx, rx) = mpsc::channel(1);
        let data = vec![7; REPLAY_LIMIT + 1];
        tx.send(data.clone()).await.unwrap();
        drop(tx);
        let mut out = router
            .start(
                StreamInput::new(rx),
                EncodingProfile::new(Codec::Mp3, 64000),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), out.rx.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            data
        );
    }
    #[tokio::test]
    async fn cancellation_stops_stalled_session_without_retry() {
        let (primary, _) = fake(Behavior::Stall);
        let (fallback, calls) = fake(Behavior::Echo);
        let router = BackendRouter::new(Some(primary), Some(fallback), vec![]);
        let (tx, rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let mut out = router
            .start(
                StreamInput::new(rx),
                EncodingProfile::new(Codec::Mp3, 64000),
                cancel.clone(),
            )
            .await
            .unwrap();
        cancel.cancel();
        assert!(tokio::time::timeout(Duration::from_secs(2), out.rx.recv())
            .await
            .unwrap()
            .is_none());
        tx.closed().await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn original_and_quality_allowlist() {
        let (primary, _) = fake(Behavior::Echo);
        let router = BackendRouter::new(Some(primary), None, vec![Quality::Low]);
        let mut p = EncodingProfile::new(Codec::Mp3, 64000);
        assert_eq!(router.backend_name_for(&p), "none");
        p.quality = Quality::Low;
        assert_eq!(router.backend_name_for(&p), "fake");
        p.quality = Quality::Original;
        assert_eq!(router.backend_name_for(&p), "passthrough");
        assert_eq!(
            parse_gst_qualities(&["low".into(), "original".into(), "bad".into()]),
            vec![Quality::Low]
        );
    }
    #[tokio::test]
    async fn missing_backends_reject_transcode_but_original_relays() {
        let router = BackendRouter::new(None, None, vec![]);
        let (_, rx) = mpsc::channel(1);
        assert!(matches!(
            router
                .start(
                    StreamInput::new(rx),
                    EncodingProfile::new(Codec::Mp3, 64000),
                    CancellationToken::new()
                )
                .await,
            Err(TranscodeError::Unavailable)
        ));
        let (tx, rx) = mpsc::channel(1);
        tx.send(vec![1, 2, 3]).await.unwrap();
        drop(tx);
        let mut p = EncodingProfile::new(Codec::Opus, 0);
        p.quality = Quality::Original;
        let mut out = router
            .start(StreamInput::new(rx), p, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(out.rx.recv().await.unwrap().unwrap(), vec![1, 2, 3]);
    }
}
