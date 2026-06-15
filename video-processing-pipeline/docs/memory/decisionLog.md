# Decision Log

## D1 — Docker build context is always `./services`

**Decision**: All Dockerfiles use `./services` as the build context, never a service subdirectory.

**Why**: Every service copies `common/` (`COPY common ./common`). If the build context were a subdirectory (e.g., `./services/generator`), the `common/` directory would be outside the context and unreachable.

**Implication**: `docker compose build` must always be run from `video-processing-pipeline/` using the `services/` context. Running `docker build` directly from inside a service directory will fail.

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

## D4 — web service consumes `tracking.combined` (Kafka Streams join)

**Decision**: `web/main.py` subscribes to `tracking.combined`. The per-topic Python merge buffer (`merge_buf`) is removed.

**Why original bypass was removed**: The `tracking-streams` app always emits for every preprocessed frame (even with 0 detections), so the LEFT JOIN is structurally complete. Buffer-aware playback (`OVERLAY_BUFFER_DELAY_S`) absorbs join latency.

**Field names**: `tracking.combined` messages already use lowercase keys (`car_tracks`, `video_timestamp_ms`, etc.). `_lower()` from `common/kafka_client.py` is kept as a defensive no-op normaliser before dispatch.

**Dual-emit**: the windowed join may emit two messages per frame (immediately with null person data, again when both sides match within the window). Browser `overlayBuffer.set(ts, payload)` overwrites, so only the final combined payload renders.

---

## D5 — Delayed `_done` signal in web

**Decision**: `web/main.py` delays its `_done` signal dispatch using `threading.Timer` with `max(MIN_DONE_DELAY_S, total_frames / DETECTION_FPS_ESTIMATE)`.

**Why**: `control.session_end` is produced by the generator when it finishes emitting frames (~8 s after upload). But at that point, the detection pipeline still has all frames queued to process. Dispatching `_done` immediately would close the WebSocket before all `tracking.combined` records for the session have arrived. `tracking-streams` keeps its per-session tracker state in a persistent state store keyed by `session_id`, so cumulative counts are unaffected by `control.session_end` timing — only the web `_done` signal needs the delay.

**Constants**:
- `DETECTION_FPS_ESTIMATE = 5.0` (CPU YOLOv8n throughput in fps)
- `MIN_DONE_DELAY_S = 30.0` (web)

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

---

## D10 — Statistics consume `tracking.combined` directly (in-memory per-session map)

**Decision**: `statistics/main.py` runs a Kafka consumer thread on `tracking.combined`. The `/stats` endpoint reads an in-memory `dict[session_id, {"unique_cars": int, "unique_persons": int}]`.

**Why**: `tracking-streams` already maintains the cumulative `cars_total` / `persons_total` unique counts per session inside its own state stores and emits the latest values on every `tracking.combined` record (`LATEST_BY_OFFSET`-style semantics via the CentroidTracker's running count). Statistics just needs to track the latest value per session, which an in-memory map does cheaply and keeps the service stateless to restart (rebuilds from the topic on next message).

**Note**: a LEFT JOIN means `persons_total` can be `None` on a record where the persons side hasn't arrived within the join window yet — the consumer keeps the previous non-null value in that case.
