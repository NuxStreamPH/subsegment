# Transcoder implementation

**What this doc is:** The engineering notes for the GStreamer-primary / FFmpeg-fallback work (the same change set as PR #3). Base repo commit [`2a5d4b6`](https://github.com/staxifrage/subsegment/commit/2a5d4b6e2007e9aefce6dd6385168b3fd3cfa901), prepared 2026-10-06. It's a handoff/audit record: how to apply the patch, what changed, what was validated, and what's explicitly out of scope.

## How to apply it

The patch ships as `subsegment-gstreamer-ffmpeg.patch` alongside full updated source. It was never pushed by the author (their account had read-only access):

```sh
git switch -c feat/gstreamer-primary-fallback
git apply --check /path/to/subsegment-gstreamer-ffmpeg.patch
git apply /path/to/subsegment-gstreamer-ffmpeg.patch
```

Then set `transcoding.backend: gstreamer` in existing FFmpeg configs. Default builds include GStreamer; `--no-default-features` builds without it.

## What changed (10 items)

1. **GStreamer backend is real now** — the old non-compiling placeholders became working in-process appsrc → decodebin → convert → encoder → appsink pipelines.
2. **Router wired into startup** — per-profile availability or pre-output failures retry through FFmpeg.
3. **Startup replay + deadline** — up to 1 MiB of input retained and replayed to the fallback; 10s first-output timeout per backend attempt; after output starts, errors close the connection instead of splicing encoders.
4. **Persistent cancellation** — flows through ingest → router → backend → manager; interrupts blocked queues/pipes, GStreamer workers join only after NULL, FFmpeg children are killed/reaped, stderr keeps draining past the log limit.
5. **Removed FFmpeg's `+nobuffer` input flag** — it could discard probed audio.
6. **Opus standardized on Ogg @ 48 kHz** with `audio/ogg` headers; headers replayed to late joiners with complete page framing.
7. **Race/cleanup fixes** — first-listener header loss, duplicate-session cleanup, shutdown races, terminal fanout closure; startup output verified before a stream response is returned as successful.
8. **Tests + CI** — media/regression tests added; Linux x86_64/ARM64 CI deps and feature checks updated.
9. **Docs** — deployment and compatibility behavior documented.
10. **Rust minimum raised 1.75 → 1.89** — to match the locked uuid dependency (the old claim was simply wrong).

## Validation actually performed

Tested on Ubuntu 24.04 x86_64, Rust 1.99, GStreamer 1.24.2, FFmpeg 6.1.1:

| Check | Result |
|---|---|
| `cargo test --locked` | ✅ 67 unit + 9 integration |
| `cargo test --locked --no-default-features` | ✅ 67 unit + 4 integration |
| `cargo check --features gstreamer_only` | ✅ |
| clippy / rustfmt / `git diff --check` | ✅ (5 pre-existing style warnings) |

**Caveats:** the GitHub Actions workflow was edited but never run remotely; ARM64 and release-profile builds weren't tested; Rust 1.89 itself wasn't tested (only 1.99).

**What the tests prove:** encoded Opus/MP3/AAC verified by real FFmpeg decode + ffprobe checks; AAC+ delegation, GStreamer bus errors, EOF, backpressure cancellation, receiver disconnect, late Opus joins, manager shutdown; router-level replay order, timeout, replay budget, disabled backends, ORIGINAL handling, quality selection, and no-retry-after-output.

## Deliberate limits (what it does *not* do)

- **No midstream failover** — failover happens only at startup; clients must reconnect after a running encoder dies.
- **No GStreamer AAC+** — AAC+ stays on FFmpeg's HE-AACv2 (only when FDK is enabled), else existing AAC-LC fallback.
- **No HLS/WMS/MMS** — only continuous HTTP byte-stream ingest (with ICY stripping).
- **No shared decode** — each quality/codec still runs its own decoder session; shared PCM multi-quality fan-out isn't implemented.
- **Opus container changed WebM → Ogg** (breaking for clients hard-coding `audio/webm`); ORIGINAL passthrough behavior unchanged.