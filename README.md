# Subsegment

A shared radio relay with in-process GStreamer transcoding and an FFmpeg fallback.
HTTP/Icecast/Shoutcast audio is ingested once per active broadcast/quality/codec
pipeline. ICY metadata is separated before decoding and listeners share encoded
output through bounded queues. ORIGINAL relays source bytes without re-encoding.

## Build and run (Ubuntu 24.04)

Install Rust 1.89 or newer and the native build/runtime dependencies:

```sh
sudo apt-get update
sudo apt-get install -y pkg-config libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev \
  gstreamer1.0-plugins-base gstreamer1.0-plugins-good gstreamer1.0-plugins-bad \
  gstreamer1.0-plugins-ugly gstreamer1.0-libav ffmpeg
cargo build --release --locked
./target/release/subsegment --config config.yaml
```

The first run generates a starter configuration if the requested file is absent.
Configure broadcasts, upstream access policy, and authentication before use.
Release binaries need the matching GStreamer/GLib shared libraries and plugins on
the destination host; runtime GStreamer initialization cannot recover from missing
shared libraries at process load time. FFmpeg must also be available on PATH (or
at `transcoding.ffmpeg_path`) for fallback. CI builds native x86_64 and ARM64 Linux
binaries and tests both media backends.

The generated configuration now defaults to:

```yaml
transcoding:
  backend: gstreamer
  gst_qualities: []
  ffmpeg_path: ffmpeg
  output_queue_len: 64
```

Existing configurations explicitly set to `backend: ffmpeg` keep that behavior;
change them to `gstreamer` to enable the primary backend. A quality allowlist such
as `[low, medium]` sends other qualities directly to FFmpeg. `passthrough_only`
allows ORIGINAL but rejects requests requiring an encoder, rather than labelling
unconverted source bytes as the requested codec.

```sh
# Portable build without any GStreamer link dependency:
cargo build --release --locked --no-default-features
# Explicitly disable FFmpeg fallback (requires backend: gstreamer):
cargo build --release --locked --features gstreamer_only
```

## Output and fallback behavior

| Request codec | GStreamer primary | FFmpeg fallback | Response MIME |
| --- | --- | --- | --- |
| Opus | opusenc → oggmux | libopus → Ogg | audio/ogg |
| MP3 | lamemp3enc → mpegaudioparse | libmp3lame | audio/mpeg |
| AAC | available fdkaacenc / avenc_aac / voaacenc → aacparse → ADTS | native AAC → ADTS | audio/aac |
| AAC+ | delegates to fallback | libfdk_aac HE-AACv2 when `aacplus_supported: true`; otherwise existing AAC-LC policy | audio/aac |
| ORIGINAL | source bytes | source bytes | upstream content type |

Opus is resampled to 48 kHz. **This changes encoded Opus from WebM to Ogg**;
clients that hard-code `audio/webm` must use the response content type. The relay
frames Ogg pages and replays Opus initialization headers to listeners joining an
existing pipeline. ORIGINAL is unchanged and does not gain container-specific
header reconstruction.

A session tries GStreamer first. Missing encoders, construction failures, decoder
errors before output, or ten seconds without initial output trigger one FFmpeg
attempt. The router retains up to 1 MiB of the original startup bytes and replays
them in order; exceeding the budget triggers fallback before consuming more into
the cache. The fallback has its own ten-second startup deadline. Only the common
GStreamer elements are checked at process startup, so a missing AAC plugin does
not disable working Opus or MP3 encoding.

After the first encoded output byte, the selected backend owns that connection.
Later errors close the stream and tear down the session; clients reconnect to
create a fresh session. Encoded containers are never spliced mid-response. This
is startup failover, not seamless recovery from a failed running encoder.

Cancellation is persistent and interrupts blocked channel writes and pipe I/O.
GStreamer reaches NULL before its workers are joined; FFmpeg is killed/reaped and
stderr continues draining after the logging limit. No transcode profile silently
falls back to raw source bytes when both encoders are unavailable.

Each quality/codec still owns a separate decode session. Shared PCM multi-quality
fan-out is not implemented. GStreamer plugins run in-process and should be kept
updated when decoding untrusted upstream media.

## Source scope

The implemented transport is the existing HTTP byte-stream ingest, including
Icecast/Shoutcast ICY stripping. Directly streamed media containers are probed by
the decoder. HLS playlist fetching/segment scheduling, DRM, and WMS/MMS transport
are **not implemented** by this change. Feeding playlist text to appsrc or FFmpeg
stdin is not HLS playback; provide a continuous audio URL instead.

## Verification

```sh
cargo test --locked
cargo test --locked --no-default-features
cargo check --locked --features gstreamer_only
```

Tests include real PCM → Opus/MP3/AAC encoding and decode checks, EOF draining,
blocked/idle cancellation, missing-backend behavior, startup timeout, bounded
input replay, no retry after output, FFmpeg handling of AAC+, and late Opus joins.
The GStreamer media tests require the packages listed above and FFmpeg/ffprobe.
