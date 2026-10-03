//! Passthrough backend: copies upstream bytes unchanged.
//!
//! Used for `ORIGINAL` quality and whenever re-encoding is disabled or
//! technically unnecessary. Consumes no transcode CPU and never counts
//! against the transcoding-pipeline limit (the manager decides that).

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, Notify};
use tracing::debug;

use super::traits::{EncodingProfile, StreamInput, TranscodeError, TranscodedStream, Transcoder};

pub struct PassthroughTranscoder;

impl PassthroughTranscoder {
    pub fn new() -> Self {
        Self
    }
}

impl Default for PassthroughTranscoder {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transcoder for PassthroughTranscoder {
    async fn start(
        &self,
        input: StreamInput,
        _profile: EncodingProfile,
        cancel: Arc<Notify>,
    ) -> Result<TranscodedStream, TranscodeError> {
        let rx = input
            .take_receiver()
            .await
            .ok_or(TranscodeError::InputClosed)?;
        let (tx, out_rx) = mpsc::channel::<Result<Vec<u8>, TranscodeError>>(64);
        debug!("passthrough pipeline started");
        tokio::spawn(async move {
            let mut rx = rx;
            loop {
                let chunk = tokio::select! {
                    c = rx.recv() => c,
                    _ = cancel.notified() => None,
                };
                match chunk {
                    Some(bytes) => {
                        if tx.send(Ok(bytes)).await.is_err() {
                            break; // consumer gone
                        }
                    }
                    None => break,
                }
            }
            let _ = tx.send(Err(TranscodeError::OutputClosed)).await;
        });
        Ok(TranscodedStream::from_receiver(out_rx))
    }

    fn name(&self) -> &'static str {
        "passthrough"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Codec, Quality};

    #[tokio::test]
    async fn copies_bytes_unchanged() {
        let (tx, rx) = mpsc::channel::<Vec<u8>>(4);
        let input = StreamInput::new(rx);
        let profile = EncodingProfile {
            codec: Codec::Auto,
            quality: Quality::Original,
            bitrate_bps: 0,
            sample_rate: 44100,
            channels: 2,
            threads: 1,
        };
        let mut out = PassthroughTranscoder::new()
            .start(input, profile, Arc::new(Notify::new()))
            .await
            .unwrap();
        tx.send(vec![1, 2, 3]).await.unwrap();
        tx.send(vec![4, 5]).await.unwrap();
        drop(tx);
        assert_eq!(out.rx.recv().await.unwrap().unwrap(), vec![1, 2, 3]);
        assert_eq!(out.rx.recv().await.unwrap().unwrap(), vec![4, 5]);
        // terminal error after EOF
        assert!(matches!(
            out.rx.recv().await,
            Some(Err(TranscodeError::OutputClosed))
        ));
    }
}
