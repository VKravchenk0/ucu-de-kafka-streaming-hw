# Capstone: E2E Video Analytics Pipeline

Kafka-based pipeline that detects and tracks cars and people in an uploaded video. The
browser starts playing the video almost immediately and overlays live bounding boxes and
running counts while detection is still catching up in the background.

---

## Quickstart

```bash
cd capstone
make infra-up     # Kafka cluster, schema registry, control center
make build-cpu    # first run only — pulls CPU torch (~200 MB), ~5-10 min
make up-cpu
```

Open http://localhost:8080 and upload a video.

---

## Architecture

### Overview

Every upload gets a `session_id` (UUID) that's used as the Kafka message key for every
message in the pipeline, so multiple uploads run side by side without interfering with
each other. The video flows through:

```
upload -> generator -> preprocessor -> {car,person}-detector (YOLOv8n)
       -> tracking-streams (tracking + join) -> web (overlays) / statistics (counts)
```

The browser plays the **original** uploaded file directly over HTTP and draws bounding
boxes on a `<canvas>` overlay, fed by a WebSocket and synced to the video's own
timestamp — no transcoded or annotated video ever goes through Kafka.

### Diagram

```mermaid
flowchart LR
    Browser["Browser<br/>video player + canvas overlay"]

    WEB["web :8080"]
    GEN[generator]
    PRE[preprocessor]
    CAR["car-detector<br/>YOLOv8n cls 2,5,7"]
    PER["person-detector<br/>YOLOv8n cls 0"]
    TS["tracking-streams<br/>CentroidTracker + windowed join"]
    STAT["statistics :8002"]

    subgraph Kafka
        CU[control.upload]
        FR[frames.raw]
        FP[frames.preprocessed]
        DC[detections.cars]
        DP[detections.persons]
        TC[tracking.combined]
        SE[control.session_end]
    end

    Browser -->|POST /upload| WEB --> CU --> GEN
    GEN --> FR --> PRE --> FP
    FP --> CAR --> DC
    FP --> PER --> DP
    GEN --> SE --> WEB
    DC --> TS
    DP --> TS
    TS --> TC --> WEB
    TC --> STAT
    WEB -->|WebSocket overlays| Browser
```

### Services

| Container | Port | Role |
|---|---|---|
| `topic-init` | — | One-shot: creates Kafka topics, exits 0 |
| `generator` | — | Reads the uploaded file frame-by-frame → `frames.raw`; emits `control.session_end` when done |
| `preprocessor` | — | Resizes frames to 640×640 → `frames.preprocessed` |
| `car-detector` | — | YOLOv8n, classes 2/5/7 (car/bus/truck) → `detections.cars` |
| `person-detector` | — | YOLOv8n, class 0 (person) → `detections.persons` |
| `tracking-streams` | — | Kafka Streams app: per-object tracking + windowed join → `tracking.combined` |
| `statistics` | 8002 | FastAPI, per-session unique car/person counts |
| `web` | 8080 | Upload UI, video serving, WebSocket overlay stream |

`car-detector` and `person-detector` share one image, differentiated by `CLASS_IDS`,
`OUTPUT_TOPIC` and `GROUP_ID`.

### Topics

| Topic | Partitions | Max msg | Producer |
|---|---|---|---|
| `control.upload` | 1 | 4 KB | web |
| `frames.raw` | 3 | 5 MB | generator |
| `frames.preprocessed` | 3 | 5 MB | preprocessor |
| `detections.cars` | 3 | 256 KB | car-detector |
| `detections.persons` | 3 | 256 KB | person-detector |
| `tracking.combined` | 3 | 256 KB | tracking-streams |
| `control.session_end` | 1 | 4 KB | generator |

`session_id` is the key on every topic, so `hash(session_id) % partitions` keeps a
session's messages on one partition without any extra coordination.

### Tracking: IoU + centroid

`tracking-streams` runs one `CentroidTracker` per object type (car, person), with each
session's state kept in a Kafka Streams state store. For every detection frame it does a
two-pass match between existing tracks and the new detections:

1. **IoU pass** — greedily pairs tracks and detections whose bounding boxes overlap by at
   least `MIN_IOU = 0.1`, highest overlap first. This keeps a track's ID stable for an
   object that's growing in frame (e.g. a car driving toward the camera), where the
   centroid barely moves but the box does.
2. **Centroid pass** — whatever's left over is matched by nearest centroid distance,
   capped at `MAX_DISTANCE = 200px`. This is the classic centroid-tracker behaviour for
   lateral motion.

Tracks unmatched for `MAX_DISAPPEARED = 30` frames are dropped; detections unmatched by
either pass become new tracks. Each tracker also keeps a cumulative set of every track ID
it has ever seen, which becomes the `*_total` (unique object) count per session.

### Kafka Streams

`tracking-streams` (`services/tracking-streams`, Java) is a single Kafka Streams app that
replaced an earlier ksqlDB-based join. Its topology:

1. Consume `detections.cars` / `detections.persons`, run the tracker above via a custom
   `Processor` backed by a persistent state store keyed by `session_id`.
2. Rekey each stream to `session_id_frame_number`.
3. Windowed `LEFT JOIN` (cars side anchored, 2s window + 500ms grace) to pair up car and
   person tracks for the same frame.
4. Rekey the joined result back to `session_id` and write it to `tracking.combined`.

This keeps tracker state, the join, and JSON (de)serialization in one JVM process — `web`
and `statistics` only ever need to read `tracking.combined`.

> Overlay synchronization and the WebSocket connection lifecycle are covered separately
> in [docs/technical.md](docs/technical.md).

---

## Build

CPU (default, no GPU needed):

```bash
make build-cpu
```

GPU (NVIDIA, CUDA 12.1 torch):

```bash
make build-gpu
```

Both produce separate image tags (`capstone-car-detector:cpu` / `:cuda`, same for
person-detector) and can coexist — build both once and switch with `make up-cpu` /
`make up-gpu` without rebuilding. GPU builds need the NVIDIA Container Toolkit configured
for Docker; see the comments in `docker-compose.gpu.yaml` for the WSL2 setup steps.

To rebuild a single service after a code change:

```bash
docker compose --profile app build --no-cache <service-name>
docker compose --profile app up -d --no-recreate
```

---

## Run

```bash
make infra-up   # Kafka cluster + schema registry + control center (once)
make up-cpu     # or: make up-gpu
```

Open http://localhost:8080, upload a video, and watch it play with overlays. Stats are at
`GET http://localhost:8080/stats` (proxied to the `statistics` service).

```bash
make logs        # follow app container logs
make down        # stop app containers
make infra-down  # stop the Kafka stack
```

Useful env vars (set in `docker-compose.yaml`):

| Var | Default | Effect |
|---|---|---|
| `FRAME_INTERVAL` | `1` | Process every Nth frame — raise to cut detector load |
| `DETECTION_FPS_ESTIMATE` | `5` | Expected detector throughput; controls how long `web` waits before ending a session |
| `OVERLAY_BUFFER_DELAY_S` | `4` | Initial/re-buffer wait in the browser player |

---

## Tests

```bash
make test
```

This brings up the full stack (`infra` + `app` profiles), uploads `test-input.mp4`,
checks that a `tracking.combined` record and a WebSocket overlay both show up for that
session, then tears the stack down. Set `E2E_KEEP_STACK=1` to leave the stack running for
inspection, or `TEST_VIDEO=/path/to/file.mp4` to use a different input.
