# System Patterns

## Message Flow

```
Browser
  │  POST /upload
  ▼
web ──► control.upload
          │
          ▼
       generator ──► frames.raw  ──► preprocessor ──► frames.preprocessed
          │                                               │           │
          ▼                                               ▼           ▼
    control.session_end                          car-detector   person-detector
          │                                          │                │
          ├──────────────────────────────────►  detections.cars  detections.persons
          │                                          │                │
          │                                          └───────┬────────┘
          │                                                   ▼
          │                                          tracking-streams
          │                                       (CentroidTracker x2 +
          │                                        windowed LEFT JOIN)
          │                                                   │
          │                                                   ▼
          │                                          tracking.combined
          │                                                   │
          │                                          ┌────────┴────────┐
          │                                          ▼                 ▼
          │                                     statistics            web
          │                                                            │
          └──────────────────────────────────────────────────────────►│
                                                                        ▼
                                                                  WebSocket
                                                                        │
                                                                        ▼
                                                                 Browser canvas
```

## Kafka Topic Design

| Topic | Partitions | Key | Notable Config |
|---|---|---|---|
| control.upload | 1 | session_id | 4 KB max |
| control.session_end | 1 | session_id | 4 KB max |
| frames.raw | 3 | session_id | 5 MB max |
| frames.preprocessed | 3 | session_id | 5 MB max |
| detections.cars | 3 | session_id | 256 KB max |
| detections.persons | 3 | session_id | 256 KB max |
| tracking.combined | 3 | session_id | 256 KB max, written by tracking-streams |

`hash(session_id) % 3` routes all messages for one session to the same partition — preserving per-session ordering without a coordinator.

## Per-Session State Pattern

All stateful services use `dict[session_id, ...]` maps (or per-key state stores) rather than global counters:
- `tracking-streams`: per-object-type Kafka Streams state stores (`car-tracker-state`, `person-tracker-state`) keyed by `session_id`, holding `CentroidTracker` state and the cumulative `total_unique` count
- `web/main.py`: `_overlay_store`, `_session_done`, `_subscribers`
- `statistics/main.py`: `_sessions: dict[str, dict]`

## Delayed Cleanup Pattern (tracker + web)

`control.session_end` arrives ~8 s after upload (generator finishes quickly), but the detection pipeline keeps running for `total_frames / DETECTION_FPS_ESTIMATE` more seconds. Both services delay state cleanup / `_done` dispatch using `threading.Timer`:

```python
delay = max(MIN_DELAY_S, total_frames / DETECTION_FPS_ESTIMATE)
threading.Timer(delay, cleanup_fn).start()
```

This prevents premature state reset (the original "cars total = cars in frame" bug).

## Web Service: Overlay Fan-Out

```
kafka_consumer_thread (background thread)
    │   consumes tracking.combined (Kafka Streams LEFT JOIN output)
    │   _lower() normalises field names (defensive no-op)
    │
    ▼
_dispatch(session_id, overlay_dict)
    ├── _overlay_store[session_id].append(item)    # durable; never deleted
    └── for q in _subscribers[session_id]:
            asyncio.run_coroutine_threadsafe(q.put(item), _loop)
                    │
                    ▼
            WebSocket handler (async)
                    │
                    ▼
               Browser
```

New WebSocket connections first replay `_overlay_store` (catch-up), then drain live from their subscriber queue.

The windowed join may emit two messages per frame (first with null person data, second with both). The browser `overlayBuffer.set(ts, payload)` overwrites, so only the final payload renders.

## Buffer-Aware Playback (Browser)

```
State: BUFFERING ──(timer + overlays present)──► PLAYING
         ▲                                           │
         └──────(currentMs > maxBufferedMs + 500)────┘
         
PLAYING ──(_done received)──► DONE (plays freely forever)

enterBuffering(): pause + spinner + N-second timer
scheduleBufferCheck(): fires after BUFFER_DELAY_MS; exits if maxBufferedMs >= currentMs - 500
exitBuffering(): hide spinner + vid.play()
```

`OVERLAY_STALE_TOLERANCE_MS = 500` prevents spurious end-of-video stalls caused by the structural gap between the last emitted overlay and `vid.duration`.

## Shared Image Pattern (detector)

Car and person variants share one Docker image with different runtime env vars:
```yaml
car-detector:
  image: capstone-car-detector:${PROCESSING_UNIT_TYPE:-cpu}
  environment:
    CLASS_IDS: "2,5,7"
    OUTPUT_TOPIC: detections.cars
    GROUP_ID: detector-cars

person-detector:
  image: capstone-person-detector:${PROCESSING_UNIT_TYPE:-cpu}
  environment:
    CLASS_IDS: "0"
    OUTPUT_TOPIC: detections.persons
    GROUP_ID: detector-persons
```

## Kafka Streams Topology (`tracking-streams`)

Implemented in `services/tracking-streams` (Java, `Main.java` / `TrackingProcessor.java`).

### Per-object-type tracking → rekey
- `detections.cars` and `detections.persons` are each processed by a `CentroidTracker` (greedy centroid matching, 30-frame disappear window) backed by a persistent state store (`car-tracker-state` / `person-tracker-state`)
- Each tracked stream is rekeyed by `session_id + "_" + frame_number`

### Windowed Stream-Stream JOIN → `tracking.combined`
- LEFT JOIN (cars anchored) within a 2-second window, 500 ms grace period
- Output rekeyed by `session_id`, written to `tracking.combined` (consumed by **both** `web` and `statistics`)
- Field names in the Kafka topic are already lowercase (`car_tracks`, `video_timestamp_ms`, `cars_total`, `persons_total`, etc.)
