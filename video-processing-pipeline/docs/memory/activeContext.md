# Active Context

## Current Branch

`capstone-streaming`

## Recently Completed Work

### Kafka Streams tracking + join pipeline
- `tracking-streams` (Java/Kafka Streams) reads `detections.cars` / `detections.persons` directly
- CentroidTracker (per object type, persistent state store) + a windowed LEFT JOIN (2s window, 500ms grace) produce `tracking.combined`
- `web/main.py` consumes `tracking.combined`; `_lower()` kept as a defensive no-op normaliser
- `statistics/main.py` consumes `tracking.combined` via a Kafka consumer thread and keeps an in-memory per-session map of `cars_total` / `persons_total`

### Buffer-aware video playback (view.html)
- Video starts paused with a spinner and "Buffering…" badge
- Initial `OVERLAY_BUFFER_DELAY_S` (default 4 s, `BUFFER_DELAY_MS` in JS) forced wait before first play attempt
- Mid-video stalls re-enter buffering when `currentMs > maxBufferedMs + threshold` (threshold = `OVERLAY_STALE_TOLERANCE_MS` only in the last 2s of video, else 0)
- Resuming requires `maxBufferedMs >= currentMs + RESUME_ADVANCE_MS` (`RESUME_ADVANCE_MS = BUFFER_DELAY_MS / 2`, i.e. 2s by default), or the pipeline has reached end-of-video / `_done` — this hysteresis stops play/pause flapping
- `_done` signal exits buffering permanently; video plays freely
- `OVERLAY_STALE_TOLERANCE_MS = 500` prevents end-of-video spurious stall

### GPU support
- `PROCESSING_UNIT_TYPE` build arg selects CPU vs CUDA torch wheels in `detector/Dockerfile`
- `docker-compose.gpu.yaml` overlay adds NVIDIA device reservation to both detectors
- Separate image tags: `car-detector:cpu` / `car-detector:cuda`
- `make build-gpu` / `make up-gpu` in Makefile; README quickstart now leads with the GPU path (CPU commands are commented alternatives), though `PROCESSING_UNIT_TYPE` still defaults to `cpu` in `docker-compose.yaml` if unset

### Removed global counters
- `statistics/main.py` previously maintained `global_cars`, `global_persons` sets
- Removed; `/stats` response is now `{"sessions": {sid: {"unique_cars": N, "unique_persons": M}}}` only — no `global` key, no per-session `status` field

### Technical docs split out (`docs/technical.md`)
- Overlay sync sequence diagram and WebSocket connection-lifecycle state diagram (both Mermaid) moved out of the README into `docs/technical.md`
- Covers `_done` timing formula and the buffering/stall hysteresis described above

### UI simplification (2026-06-13 → 2026-06-15)
- `index.html` title changed to "Video Stream Processing Pipeline"
- `view.html` header simplified: removed the "Live Analytics" `<h1>` and the `WS: connecting/live/closed` status indicator entirely; "Upload another" → "Upload"
- Session stats panel heading "This Session" → "Session Stats"; `#info-box` starts empty and is populated by JS instead of a hardcoded "Buffering…" message

## Current Known Issues / Watch Points

- **`_done` delay is CPU-fixed**: `DETECTION_FPS_ESTIMATE` defaults to 5 fps even when running GPU. With GPU (~80–200 fps), `max(30, total_frames/5)` still waits 30–46 s unnecessarily. Workaround: set `DETECTION_FPS_ESTIMATE` env var to a higher value when using GPU.

- **`_overlay_store` is never deleted** within a web process lifetime. Long-running servers accumulate per-session overlay lists indefinitely (memory leak for many sessions).

## Files Most Likely to Need Changes

| File | Why |
|---|---|
| `services/web/templates/view.html` | UI/UX changes, overlay rendering, buffer logic |
| `services/web/main.py` | WebSocket lifecycle, `_done` timing, new routes |
| `services/tracking-streams/src/main/java/ucu/de/dsk/capstone/tracking/` | Tracking and join logic |
| `docker-compose.yaml` | Env var tuning (FRAME_INTERVAL, OVERLAY_BUFFER_DELAY_S) |

## How to Test After Changes

```bash
# Rebuild and restart only the changed service (fastest)
cd video-processing-pipeline
docker compose --profile app build web && docker compose --profile app up -d web

# Full restart
make down && make up-cpu

# Verify overlays and stats
curl -s http://localhost:8080/stats | python3 -m json.tool

# Follow logs
make logs
```
