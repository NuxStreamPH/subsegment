# subsegment

**What it is:** A Rust radio-stream relay that NuxStream Radio uses. It pulls one HTTP/Icecast/Shoutcast/HLS stream per station, transcodes it in-process with **GStreamer** (FFmpeg as fallback), and serves multiple output qualities/codecs to listeners. `ORIGINAL` quality just relays source bytes untouched.

## Quick start (Ubuntu 24.04)

1. Install Rust ≥ 1.89 + GStreamer/FFmpeg packages (listed in README).
2. `cargo build --release --locked` → run `./target/release/subsegment --config config.yaml` (auto-generates a starter config on first run).
3. Deploy notes: the binary needs matching GStreamer/GLib libs on the target host; FFmpeg must be on PATH for fallback.

**Build variants:**
- Default = GStreamer + FFmpeg fallback
- `--no-default-features` = FFmpeg-only, no GStreamer link dependency
- `--features gstreamer_only` = GStreamer only (disables FFmpeg)

## Defaults & config essentials

```yaml
transcoding:
  backend: gstreamer   # existing configs set to "ffmpeg" keep working
  gst_qualities: []    # e.g. [low, medium] → other qualities go to FFmpeg
```

`passthrough_only` allows only ORIGINAL rather than faking a codec.

## What each codec does

| Codec | Path | MIME |
|---|---|---|
| Opus | GStreamer opusenc/oggmux, FFmpeg libopus | `audio/ogg` (⚠️ changed from WebM) |
| MP3 | lamemp3enc / libmp3lame | `audio/mpeg` |
| AAC | fdkaacenc/avenc_aac/voaacenc / native | `audio/aac` |
| AAC+ | always via FFmpeg (HE-AACv2 if enabled) | `audio/aac` |
| ORIGINAL | byte-for-byte relay | upstream type |

## Key behaviors to know

- **Startup failover, not midstream:** GStreamer is tried first; FFmpeg kicks in only if startup fails (missing encoder, decoder error, or 10s without output). Up to 1 MiB of startup bytes is replayed to the fallback. Once output starts, that backend owns the connection — later errors close the stream and clients reconnect.
- **Opus is now Ogg @ 48 kHz** — clients hard-coding `audio/webm` must read the response Content-Type. Init headers are replayed to late joiners.
- **One download per station:** MP3/ADTS AAC sources are fetched once and shared across all profiles (3 profiles of a 320 kbps source ≈ 320 kbps ingress, not 960). Ogg/MP4/unknown containers keep per-profile downloads.
- **No total stream timeout:** `upstream_read_timeout_secs` (30s) is an *inactivity* timeout — healthy streams stay connected forever. Reconnects use capped exponential backoff + jitter.
- **Bounded queues:** each encoder gets its own queue (64 × ≤64 KiB chunks ≈ max 4 MiB). If one output backs up, only that branch is cancelled — others keep playing. Concurrent identical requests share one encoder startup.
- **Cleanup is reliable:** cancellation interrupts blocked I/O, GStreamer pipelines reach NULL, FFmpeg children are killed/reaped, and no profile ever silently serves raw source bytes as a fake transcode.

## Useful limits (all auto-defaulted)

```yaml
limits:
  share_upstream: true                  # set false to diagnose source issues
  ingest_queue_chunks: 64               # 8–1024
  pipeline_startup_timeout_secs: 60     # full startup budget incl. retries
  upstream_connect_timeout_secs: 10
  upstream_read_timeout_secs: 30        # inactivity, not total
```

**Metrics to watch:** `subsegment_upstream_connections_active`, `..._reconnects_total`, `..._bytes_received_total`, `..._ingest_branch_drops_total` (last one flags encoders that can't keep up). Post-deploy: watch one station ≥10 min and request multiple profiles.

## Not supported

HLS playlists/segments, DRM, WMS/MMS — it only ingests continuous HTTP audio streams. Feed it a direct stream URL, not a playlist.

## Verification

```sh
cargo test --locked                        # GStreamer build
cargo test --locked --no-default-features  # FFmpeg-only build
cargo check --locked --features gstreamer_only
```

Tests include real encode→decode verification, late-join header replay, backpressure/cancellation, startup timeout, and shared-ingest regression tests (GStreamer media tests need the native packages + FFmpeg/ffprobe).