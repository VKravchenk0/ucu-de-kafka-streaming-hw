# Progress

## What Works

### Core Pipeline
- [x] Video upload via browser (drag-and-drop + click)
- [x] Frame extraction with configurable `FRAME_INTERVAL` (currently 3)
- [x] JPEG encoding, base64 framing, Kafka fan-out
- [x] Preprocessor resizes frames to 640×640
- [x] YOLOv8n inference (CPU default, GPU optional)
- [x] CentroidTracker per session (greedy centroid matching, 30-frame disappear window)
- [x] `tracking.cars` and `tracking.persons` topics populated correctly
- [x] ksqlDB LEFT JOIN producing `tracking.combined`
- [x] Per-session statistics via `GET /stats`

### Web / Browser
- [x] WebSocket overlay streaming with catch-up replay on reconnect
- [x] Per-session `_overlay_store` (durable, never deleted)
- [x] Canvas bounding-box rendering, scaled from 640×640 to display size
- [x] Track IDs shown in colored label chips (orange=car, blue=person)
- [x] Buffer-aware playback: spinner + "Buffering…" badge
- [x] Mid-video re-buffering when pipeline stalls
- [x] `OVERLAY_STALE_TOLERANCE_MS = 500` prevents end-of-video hang
- [x] `_done` signal clears buffering state permanently
- [x] WebSocket auto-reconnect (exponential backoff, 5 retries)

### Infrastructure & DevOps
- [x] `PROCESSING_UNIT_TYPE=cpu/cuda` GPU/CPU switching at build time
- [x] `docker-compose.gpu.yaml` NVIDIA device reservation overlay
- [x] `Makefile` with `build-cpu/gpu`, `up-cpu/gpu`, `down`, `logs`
- [x] CPU and GPU detector images coexist with separate tags
- [x] Prometheus + Alertmanager + Control Center in infra profile

### Correctness
- [x] Per-session unique object counts (not global)
- [x] Premature session cleanup fixed with `threading.Timer` delay
- [x] `produce_with_backpressure` prevents `BufferError` under load
- [x] `session_id` as Kafka key ensures per-session partition ordering

## What Does Not Work / Known Gaps

- [ ] `DETECTION_FPS_ESTIMATE` is hard-coded to CPU speed (5 fps) — `_done` delay is unnecessarily long when running GPU; no env var override documented at runtime
- [ ] `_overlay_store` memory leak for long-running servers with many sessions — no TTL or eviction policy
- [ ] Statistics service (`/stats`) depends on ksqlDB join; if ksqlDB lags, stats lag too
- [ ] No authentication or session isolation — anyone can view any session ID
- [ ] `findClosestOverlay` is O(n) per animation frame — could be slow with very long videos

## Milestone Status

| Milestone | Status |
|---|---|
| Basic E2E pipeline (upload → detect → overlay) | Complete |
| Per-session tracking (not global) | Complete |
| GPU support | Complete |
| Buffer-aware playback (spinner, stall recovery) | Complete |
| Frame skipping for performance | Complete |
| README + Makefile for easy startup | Complete |
| Memory bank docs | In progress |
