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
- Initial `OVERLAY_BUFFER_DELAY_S` (default 4 s) forced wait before first play attempt
- Mid-video stalls trigger another N-second re-buffer wait when `currentMs > maxBufferedMs + 500`
- `_done` signal exits buffering permanently; video plays freely
- `OVERLAY_STALE_TOLERANCE_MS = 500` prevents end-of-video spurious stall

### GPU support
- `PROCESSING_UNIT_TYPE` build arg selects CPU vs CUDA torch wheels in `detector/Dockerfile`
- `docker-compose.gpu.yaml` overlay adds NVIDIA device reservation to both detectors
- Separate image tags: `car-detector:cpu` / `car-detector:cuda`
- `make build-gpu` / `make up-gpu` in Makefile

### Removed global counters
- `statistics/main.py` previously maintained `global_cars`, `global_persons` sets
- Removed; `/stats` response now only contains per-session data

### Frame skipping
- `FRAME_INTERVAL` set to `"3"` in `docker-compose.yaml` (was `"1"`)
- Reduces frames processed from ~695 to ~232 for a 23-second video at 30fps
- Cuts detection pipeline load ~3×

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
