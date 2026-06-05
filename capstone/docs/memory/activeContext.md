# Active Context

## Current Branch

`capstone-streaming`

## Recently Completed Work

### ksqlDB-first streaming refactor
- `statistics/main.py` replaced Kafka consumer + Python sets with ksqlDB pull queries
- `ksql_init/main.py` adds `session_car_stats` and `session_person_stats` CTAS tables (`LATEST_BY_OFFSET(total_unique)`)
- `web/main.py` now consumes `tracking.combined`; `merge_buf` removed; `_lower()` normalises UPPERCASE keys
- `common/kafka_client.py` gains `_lower()` utility (shared by web + statistics)
- `statistics/requirements.txt` adds `requests`; `KAFKA_BOOTSTRAP_SERVERS` env replaced by `KSQLDB_URL`

### Buffer-aware video playback (view.html)
- Video starts paused with a spinner and "Buffering…" badge
- Initial `OVERLAY_BUFFER_DELAY_S` (default 4 s) forced wait before first play attempt
- Mid-video stalls trigger another N-second re-buffer wait when `currentMs > maxBufferedMs + 500`
- `_done` signal exits buffering permanently; video plays freely
- `OVERLAY_STALE_TOLERANCE_MS = 500` prevents end-of-video spurious stall

### GPU support
- `PROCESSING_UNIT_TYPE` build arg selects CPU vs CUDA torch wheels in `detector/Dockerfile`
- `docker-compose.gpu.yaml` overlay adds NVIDIA device reservation to both detectors
- Separate image tags: `capstone-car-detector:cpu` / `capstone-car-detector:cuda`
- `make build-gpu` / `make up-gpu` in Makefile

### Session tracking fix
- Premature `control.session_end` was wiping `seen` sets mid-stream in tracker
- Fixed with `threading.Timer(max(30, total_frames/5), cleanup_fn)` in `tracker/main.py`
- Same delay pattern applied to `_done` dispatch in `web/main.py`

### Removed global counters
- `statistics/main.py` previously maintained `global_cars`, `global_persons` sets
- Removed; `/stats` response now only contains per-session data

### Frame skipping
- `FRAME_INTERVAL` set to `"3"` in `docker-compose.yaml` (was `"1"`)
- Reduces frames processed from ~695 to ~232 for a 23-second video at 30fps
- Cuts detection pipeline load ~3×

## Current Known Issues / Watch Points

- **`_done` delay is CPU-fixed**: `DETECTION_FPS_ESTIMATE` defaults to 5 fps even when running GPU. With GPU (~80–200 fps), `max(30, total_frames/5)` still waits 30–46 s unnecessarily. Workaround: set `DETECTION_FPS_ESTIMATE` env var to a higher value when using GPU.

- **Statistics and web both depend on ksqlDB**: if ksqlDB is down, `/stats` returns 503 and web overlays will lag or be absent. Previously the web overlay bypassed ksqlDB.

- **`_overlay_store` is never deleted** within a web process lifetime. Long-running servers accumulate per-session overlay lists indefinitely (memory leak for many sessions).

## Files Most Likely to Need Changes

| File | Why |
|---|---|
| `services/web/templates/view.html` | UI/UX changes, overlay rendering, buffer logic |
| `services/web/main.py` | WebSocket lifecycle, `_done` timing, new routes |
| `services/tracker/main.py` | Tracking logic, session cleanup timing |
| `docker-compose.yaml` | Env var tuning (FRAME_INTERVAL, OVERLAY_BUFFER_DELAY_S) |

## How to Test After Changes

```bash
# Rebuild and restart only the changed service (fastest)
cd capstone
docker compose --profile app build web && docker compose --profile app up -d web

# Full restart
make down && make up-cpu

# Verify overlays and stats
curl -s http://localhost:8080/stats | python3 -m json.tool

# Follow logs
make logs
```
