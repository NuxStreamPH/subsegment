//! In-process, bounded appsrc → decoder → encoder → appsink sessions.
//! Blocking GStreamer operations run off the Tokio worker threads. A single
//! supervisor owns teardown, puts the pipeline in NULL, and joins both workers.

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use gst::prelude::*;
use gstreamer as gst;
use gstreamer_app::{AppSink, AppSrc};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::{EncodingProfile, StreamInput, TranscodeError, TranscodedStream, Transcoder};
use crate::config::TranscodingConfig;
use crate::telemetry::metrics;
use crate::types::Codec;

fn initialize() -> Result<(), TranscodeError> {
    static INIT: OnceLock<Result<(), String>> = OnceLock::new();
    INIT.get_or_init(|| gst::init().map_err(|e| e.to_string()))
        .as_ref()
        .map(|_| ())
        .map_err(|_| TranscodeError::Unavailable)
}

/// Only probe common elements here. Missing codec plugins must not disable
/// the whole backend: individual sessions can fall back independently.
pub fn available() -> bool {
    initialize().is_ok()
        && [
            "appsrc",
            "decodebin",
            "audioconvert",
            "audioresample",
            "queue",
            "appsink",
        ]
        .iter()
        .all(|name| gst::ElementFactory::find(name).is_some())
}

fn pipeline_description(p: &EncodingProfile) -> Result<String, TranscodeError> {
    if p.channels == 0 || p.channels > 2 || p.sample_rate == 0 || p.bitrate_bps == 0 {
        return Err(TranscodeError::Unavailable);
    }
    let tail = match p.codec {
        Codec::Opus => format!(
            "opusenc bitrate={} ! oggmux max-delay=100000000 max-page-delay=100000000",
            p.bitrate_bps
        ),
        Codec::Mp3 | Codec::Auto => format!(
            "lamemp3enc target=bitrate bitrate={} cbr=true ! mpegaudioparse",
            p.bitrate_bps / 1000
        ),
        Codec::Aac => {
            let encoder = ["fdkaacenc", "avenc_aac", "voaacenc"]
                .into_iter()
                .find(|name| gst::ElementFactory::find(name).is_some())
                .ok_or(TranscodeError::Unavailable)?;
            format!("{encoder} bitrate={} ! audio/mpeg,mpegversion=4,base-profile=lc ! aacparse ! audio/mpeg,mpegversion=4,stream-format=adts", p.bitrate_bps)
        }
        // HE-AAC capabilities differ across plugin builds. Keep the existing
        // configured FFmpeg HE-AAC/AAC-LC policy instead of silently claiming HE.
        Codec::AacPlus => return Err(TranscodeError::Unavailable),
    };
    let rate = if p.codec == Codec::Opus {
        48000
    } else {
        p.sample_rate
    };
    Ok(format!(
        "appsrc name=input block=true max-bytes=262144 format=bytes ! decodebin ! \
         audioconvert ! audioresample ! audio/x-raw,rate={rate},channels={} ! \
         queue max-size-buffers=0 max-size-bytes=262144 max-size-time=2000000000 ! \
         {tail} ! appsink name=output sync=false max-buffers=8 drop=false wait-on-eos=false",
        p.channels
    ))
}

pub struct GstreamerTranscoder {
    cfg: Arc<TranscodingConfig>,
}

impl GstreamerTranscoder {
    pub fn new(cfg: Arc<TranscodingConfig>) -> Self {
        Self { cfg }
    }
}

#[async_trait]
impl Transcoder for GstreamerTranscoder {
    async fn start(
        &self,
        input: StreamInput,
        profile: EncodingProfile,
        cancel: CancellationToken,
    ) -> Result<TranscodedStream, TranscodeError> {
        initialize()?;
        let started = Instant::now();
        let description = pipeline_description(&profile)?;
        let pipeline = gst::parse::launch(&description)
            .map_err(|e| {
                warn!(error = %e, "GStreamer pipeline unavailable");
                TranscodeError::Unavailable
            })?
            .downcast::<gst::Pipeline>()
            .map_err(|_| TranscodeError::Unavailable)?;
        let appsrc = pipeline
            .by_name("input")
            .and_then(|e| e.downcast::<AppSrc>().ok())
            .ok_or(TranscodeError::Unavailable)?;
        let appsink = pipeline
            .by_name("output")
            .and_then(|e| e.downcast::<AppSink>().ok())
            .ok_or(TranscodeError::Unavailable)?;
        let bus = pipeline.bus().ok_or(TranscodeError::Unavailable)?;
        if pipeline.set_state(gst::State::Playing).is_err() {
            let _ = pipeline.set_state(gst::State::Null);
            return Err(TranscodeError::Unavailable);
        }
        metrics::TRANSCODE_COLDSTART_SECONDS
            .with_label_values(&["gstreamer"])
            .observe(started.elapsed().as_secs_f64());
        let (tx, out_rx) = mpsc::channel(self.cfg.output_queue_len.max(4));
        let stop = cancel.child_token();
        let pump_stop = stop.clone();
        let mut input = input.into_receiver();
        let runtime = tokio::runtime::Handle::current();
        let mut pump = tokio::task::spawn_blocking(move || -> Result<(), TranscodeError> {
            loop {
                let chunk = runtime.block_on(async {
                    tokio::select! {
                        biased;
                        _ = pump_stop.cancelled() => None,
                        chunk = input.recv() => chunk,
                    }
                });
                match chunk {
                    Some(bytes) => {
                        appsrc
                            .push_buffer(gst::Buffer::from_slice(bytes))
                            .map_err(|_| TranscodeError::ProcessFailed)?;
                    }
                    None => {
                        if !pump_stop.is_cancelled() {
                            appsrc
                                .end_of_stream()
                                .map_err(|_| TranscodeError::ProcessFailed)?;
                        }
                        return Ok(());
                    }
                }
            }
        });
        let pull_stop = stop.clone();
        let pull_tx = tx.clone();
        let runtime = tokio::runtime::Handle::current();
        let mut puller = tokio::task::spawn_blocking(move || -> Result<(), TranscodeError> {
            let mut first = true;
            while !pull_stop.is_cancelled() {
                if let Some(sample) = appsink.try_pull_sample(gst::ClockTime::from_mseconds(100)) {
                    let buffer = sample.buffer().ok_or(TranscodeError::ProcessFailed)?;
                    let map = buffer
                        .map_readable()
                        .map_err(|_| TranscodeError::ProcessFailed)?;
                    if map.as_slice().is_empty() {
                        continue;
                    }
                    let bytes = map.as_slice().to_vec();
                    if first {
                        metrics::TRANSCODE_FIRSTBYTE_SECONDS
                            .with_label_values(&["gstreamer"])
                            .observe(started.elapsed().as_secs_f64());
                        first = false;
                    }
                    let sent = runtime.block_on(async {
                        tokio::select! {
                            biased;
                            _ = pull_stop.cancelled() => false,
                            sent = pull_tx.send(Ok(bytes)) => sent.is_ok(),
                        }
                    });
                    if !sent {
                        return Ok(());
                    }
                } else if appsink.is_eos() {
                    return Ok(());
                }
            }
            Ok(())
        });
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(50));
            let mut puller_done = false;
            let mut pump_done = false;
            let mut result = loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break Ok(()),
                    _ = tx.closed() => break Ok(()),
                    result = &mut pump, if !pump_done => {
                        pump_done = true;
                        if !matches!(result, Ok(Ok(()))) {
                            break Err(TranscodeError::ProcessFailed);
                        }
                    }
                    result = &mut puller => {
                        puller_done = true;
                        break result.unwrap_or(Err(TranscodeError::ProcessFailed));
                    }
                    _ = tick.tick() => {
                        if let Some(message) = bus.timed_pop_filtered(gst::ClockTime::ZERO, &[gst::MessageType::Error]) {
                            if let gst::MessageView::Error(e) = message.view() {
                                warn!(error = %e.error(), "GStreamer session failed");
                            }
                            break Err(TranscodeError::ProcessFailed);
                        }
                    }
                }
            };
            // Error and EOS can race: do not let appsink EOS hide a bus error.
            if bus
                .timed_pop_filtered(gst::ClockTime::ZERO, &[gst::MessageType::Error])
                .is_some()
            {
                result = Err(TranscodeError::ProcessFailed);
            }
            stop.cancel();
            // NULL unblocks push_buffer; stop unblocks receive/send. Always join.
            let _ = tokio::task::spawn_blocking(move || pipeline.set_state(gst::State::Null)).await;
            if !pump_done {
                let _ = pump.await;
            }
            if !puller_done {
                let _ = puller.await;
            }
            if let Err(e) = result {
                tokio::select! {
                    _ = cancel.cancelled() => {},
                    _ = tx.send(Err(e)) => {},
                }
            }
        });
        Ok(TranscodedStream::from_receiver(out_rx))
    }

    fn name(&self) -> &'static str {
        "gstreamer"
    }
}
