# GStreamer primary / FFmpeg fallback integration

Base repository: https://github.com/staxifrage/subsegment
Base commit: `2a5d4b6e2007e9aefce6dd6385168b3fd3cfa901`
Prepared: 2026-10-06

The archive contains the updated source under `subsegment/` and an apply-ready
`subsegment-gstreamer-ffmpeg.patch` alongside it. No change was pushed to GitHub:
the connected account reported read-only repository access.

To apply the patch to an existing clean checkout of the base commit:

```sh
git switch -c feat/gstreamer-primary-fallback
git apply --check /path/to/subsegment-gstreamer-ffmpeg.patch
git apply /path/to/subsegment-gstreamer-ffmpeg.patch
```

Alternatively, use the supplied complete source directory. Follow README.md for
native dependencies, configuration, build commands, and runtime requirements.
Set `transcoding.backend: gstreamer` in an existing FFmpeg configuration. The
new default build includes GStreamer; `--no-default-features` builds without it.

## What changed

- Replaced the non-compiling GStreamer placeholders with in-process appsrc /
  decodebin / audio conversion / encoder / appsink sessions.
- Wired the backend router into the crate and server startup. Per-profile
  availability failures and failures before output retry through FFmpeg.
- Added bounded startup replay (1 MiB) and a ten-second first-output timeout per
  backend attempt. Once output begins, errors end the connection instead of
  concatenating incompatible encoder sessions.
- Added persistent cancellation through ingest, router, backend, and manager;
  blocked queues/pipes are interrupted, GStreamer workers are joined after NULL,
  and FFmpeg children are killed and reaped. Stderr drains beyond its log limit.
- Removed FFmpeg's input `+nobuffer` flag, which could discard probed audio.
- Standardized Opus on Ogg with 48 kHz sampling and `audio/ogg` response headers.
  Ogg headers are retained for late listeners, with complete page framing.
- Fixed first-listener header loss, duplicate-session cleanup, shutdown races,
  and terminal fanout closure. Startup output is checked before returning a
  successful stream response.
- Added media/regression tests, updated Linux x86_64/ARM64 CI dependencies and
  feature checks, and documented deployment and compatibility behavior.
- Raised the declared Rust minimum to 1.89, matching the existing locked uuid
  dependency rather than claiming the original incompatible 1.75 minimum.

## Validation actually performed

Environment: Ubuntu 24.04 x86_64, Rust/Cargo 1.99.0, GStreamer 1.24.2,
FFmpeg 6.1.1. Native libraries were extracted into a local dependency directory
for this execution environment; normal hosts can use the apt instructions.

| Check | Result |
| --- | --- |
| `cargo test --locked` | PASS: 67 unit + 9 integration tests |
| `cargo test --locked --no-default-features` | PASS: 67 unit + 4 integration tests |
| `cargo check --locked --features gstreamer_only` | PASS |
| `cargo clippy --locked --all-targets` | PASS with 5 existing style warnings |
| rustfmt on changed Rust files | PASS |
| `git diff --check` | PASS |

Media tests verify encoded Opus/MP3/AAC streams by decoding with FFmpeg and
checking codec/sample-rate with ffprobe. They also cover AAC+ delegation,
GStreamer decoder bus errors, EOF, cancellation under backpressure, output
receiver disconnect, late Opus joins, and manager shutdown against a local HTTP
source. Router tests cover retained input order, timeout, replay budget, disabled
backends, ORIGINAL, quality selection, and no retry after committed output.

The GitHub Actions workflow was edited but not run remotely. ARM64 and a release
optimization build were not executed here. Rust 1.89 itself was not tested.

## Deliberate limits

- Failover is during startup, not seamless midstream recovery. Clients reconnect
  after a running encoder fails.
- AAC+ uses the existing FFmpeg HE-AACv2 policy when FDK is explicitly enabled;
  otherwise it retains the existing AAC-LC fallback. GStreamer AAC+ is not claimed.
- HLS playlist resolution/segment fetching and WMS/MMS transport remain outside
  the existing continuous HTTP audio ingest. They are not implemented here.
- Each quality/codec still has its own decode session; shared PCM multi-quality
  fanout is not implemented.
- Encoded Opus changed from WebM to Ogg. ORIGINAL passthrough is unchanged and
  does not reconstruct arbitrary source-container initialization headers.
