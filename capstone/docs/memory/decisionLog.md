# Decision Log

## D1 — Docker build context is always `./services`

**Decision**: All Dockerfiles use `./services` as the build context, never a service subdirectory.

**Why**: Every service copies `common/` (`COPY common ./common`). If the build context were a subdirectory (e.g., `./services/generator`), the `common/` directory would be outside the context and unreachable.

**Implication**: `docker compose build` must always be run from `capstone/` using the `services/` context. Running `docker build` directly from inside a service directory will fail.

---

## D2 — torch/torchvision must NOT appear in requirements.txt

**Decision**: `torch` and `torchvision` are installed via explicit `--index-url` in `detector/Dockerfile`, not listed in `requirements.txt`.

**Why**: pip resolves packages from PyPI by default. The CUDA build of torch comes from `https://download.pytorch.org/whl/cu121`. Without the index URL, pip pulls the GPU build on CPU hosts (pulling ~1.5 GB unnecessarily) or vice versa. There is no way to express the conditional index URL in `requirements.txt`.

**Pattern**:
```dockerfile
ARG PROCESSING_UNIT_TYPE=cpu
RUN if [ "$PROCESSING_UNIT_TYPE" = "cuda" ]; then \
      pip install torch torchvision --index-url https://download.pytorch.org/whl/cu121; \
    else \
      pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu; \
    fi
```

---

## D3 — GPU/CPU is a build-time, not runtime, selection

**Decision**: `PROCESSING_UNIT_TYPE` is a Docker build ARG, not a pure runtime env var (though it is also passed at runtime to tell ultralytics which device to use).

**Why**: CPU-only and CUDA torch are different PyPI packages. Switching devices at runtime would require reinstalling torch inside the running container. Building two distinct images (one per value) is the standard pattern.

**Implication**: Changing `PROCESSING_UNIT_TYPE` requires a full rebuild of detector images.

---

## D4 — web service reads tracking topics directly, bypassing ksqlDB

**Decision**: `web/main.py` subscribes to `tracking.cars` and `tracking.persons` directly. It does NOT consume `tracking.combined`.

**Why**: The ksqlDB stream-stream LEFT JOIN has inherent latency (windowed join requires waiting for the window to close + grace period). Consuming `tracking.combined` in the web service caused frames to arrive significantly delayed or missing, breaking the overlay sync. Direct topic consumption gives the lowest possible latency.

**Implication**: The web merge buffer (`merge_buf` in `kafka_consumer_thread`) does its own car+person merge by `video_timestamp_ms`. The ksqlDB join is only used by the `statistics` service.

---

## D5 — Delayed `_done` signal and tracker cleanup

**Decision**: Both `web/main.py` and `tracker/main.py` delay their cleanup / signal dispatch using `threading.Timer` with `max(MIN_DELAY_S, total_frames / DETECTION_FPS_ESTIMATE)`.

**Why**: `control.session_end` is produced by the generator when it finishes emitting frames (~8 s after upload). But at that point, the detection pipeline still has all frames queued to process. Original code processed `session_end` immediately, which caused the tracker's `seen` sets to be wiped while detections were still arriving — producing `cars_total = cars_in_frame` (the tracking reset bug).

**Constants**:
- `DETECTION_FPS_ESTIMATE = 5.0` (CPU YOLOv8n throughput in fps)
- `MIN_DONE_DELAY_S = 30.0` (web), `MIN_CLEANUP_DELAY_S = 30.0` (tracker)

---

## D6 — `session_id` as Kafka message key

**Decision**: All messages use `session_id` (UUID string) as the Kafka key.

**Why**: `hash(session_id) % num_partitions` routes all messages for one session to the same partition, preserving per-session ordering across all topics without needing a central coordinator. Services can process messages in frame order without buffering for cross-partition reordering.

---

## D7 — `produce_with_backpressure` wrapper

**Decision**: All producers use `produce_with_backpressure()` from `common/kafka_client.py` instead of calling `producer.produce()` directly.

**Why**: When the producer internal queue is full (typically under burst load with large JPEG frames), `producer.produce()` raises `BufferError`. The wrapper retries with `producer.poll()` between attempts to drain the queue before re-trying.

---

## D8 — `OVERLAY_STALE_TOLERANCE_MS = 500` in view.html

**Decision**: The buffering trigger condition is `currentMs > maxBufferedMs + 500` rather than `currentMs > maxBufferedMs`.

**Why**: With `FRAME_INTERVAL=3` at 30fps, overlays arrive every ~100ms. The last overlay's timestamp is structurally 100–300ms before `vid.duration`. Without tolerance, the video would stall for `BUFFER_DELAY_MS` (4 s) at the very end of every video and wait for `_done` (which has a 30-second minimum delay). 500ms covers the structural gap without allowing real mid-video stalls (which are seconds behind) to pass through.

---

## D9 — `_overlay_store` is durable (never deleted)

**Decision**: `_overlay_store[session_id]` in `web/main.py` is never deleted during the process lifetime.

**Why**: A second browser tab or page reload should immediately get all overlays for a session without re-processing. The catch-up phase replays the full store. Deleting the store on `_done` would break reconnect scenarios.

**Trade-off**: Memory accumulates per session. Acceptable for development/demo use; a production system would need TTL eviction.
