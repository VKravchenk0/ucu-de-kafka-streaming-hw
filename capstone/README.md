# Capstone: E2E Video Stream Processing Pipeline

Kafka-based video pipeline that accepts uploaded videos, detects and tracks cars and people using YOLOv8, overlays bounding boxes on the live stream, and reports per-session and global unique-object statistics. Supports multiple simultaneous uploads from different browsers.

---

## Architecture

```
Browser ──── POST /upload ────────────────────────────────────────────────┐
             GET  /view/{session_id}  (MJPEG stream + live stats)         │
             GET  /stream/{session_id}                                     │
             GET  /stats                                                   │
                                                                           ▼
                                                                    ┌────────────┐
                                                                    │    web     │
                                                                    │  :8080     │
                                                                    └──────┬─────┘
                                                     stats proxy           │  saves file to /uploads volume
                                                  ┌──────────────────      │  publishes control.upload
                                                  ▼                        ▼
                                         ┌──────────────┐     topic: control.upload
                                         │  statistics  │         {session_id, file_path}
                                         │  :8002/stats │                  │
                                         └──────┬───────┘                  ▼
                                                │                    ┌─────────────┐
                                                │                    │  generator  │
                                                │                    │  (threaded) │
                                                │                    └──────┬──────┘
                                                │                           │ frames.raw
                                                │                           │ {session_id, frame_number, ...}
                                                │                           ▼
                                                │                    ┌──────────────┐
                                                │                    │ preprocessor │
                                                │                    │ resize 640²  │
                                                │                    └──────┬───────┘
                                                │                           │ frames.preprocessed
                                                │                    ┌──────┴──────┐
                                                │                    ▼             ▼
                                                │             ┌────────────┐ ┌──────────────┐
                                                │             │car-detector│ │person-detect.│
                                                │             │ YOLOv8n   │ │  YOLOv8n     │
                                                │             │ cls 2,5,7 │ │  cls 0       │
                                                │             └─────┬──────┘ └──────┬───────┘
                                                │                   │               │
                                                │        detections.cars   detections.persons
                                                │                   │               │
                                                │             ┌─────┴──────┐ ┌──────┴───────┐
                                                │             │ car-tracker│ │person-tracker│
                                                │             │ {sid:      │ │ {sid:        │
                                                │             │  Tracker}  │ │  Tracker}    │
                                                │             └─────┬──────┘ └──────┬───────┘
                                                │                   │               │
                                                │         tracking.cars    tracking.persons
                                                │            │      │          │      │
                                                └────────────┘      └──────────┘      │
                                            (per-session +                  ▼
                                             global stats)           ┌─────────────┐
                                                                     │  renderer   │
                                                            frames.  │ joins frame │
                                                         preprocessed│ + cars      │
                                                              ──────►│ + persons   │
                                                                     │ draws boxes │
                                                                     └──────┬──────┘
                                                                            │ frames.rendered
                                                                            ▼
                                                                    ┌────────────┐
                                                                    │    web     │
                                                                    │ /stream/   │
                                                                    │ {session}  │
                                                                    └──────┬─────┘
                                                                           │ MJPEG
                                                                           ▼
                                                                        Browser
```

---

## Services

| Service | Profile | Port | Image | Description |
|---|---|---|---|---|
| `topic-init` | app | — | 138 MB | One-shot: creates all 9 Kafka topics, then exits |
| `generator` | app | — | 563 MB | Listens on `control.upload`; processes each video in its own thread; embeds `session_id` in every frame |
| `preprocessor` | app | — | 563 MB | Resizes frames to 640×640, forwards `session_id` |
| `car-detector` | app | — | 1.9 GB | YOLOv8n CPU, COCO classes {2=car, 5=bus, 7=truck} |
| `person-detector` | app | — | 1.9 GB | YOLOv8n CPU (same image), COCO class {0=person} |
| `car-tracker` | app | — | 138 MB | Per-session centroid tracker; cleans up on `control.session_end` |
| `person-tracker` | app | — | 138 MB | Same image, different `OBJECT_TYPE` env var |
| `statistics` | app | 8002 | 138 MB | Per-session + global unique counts; HTTP `GET /stats` |
| `renderer` | app | — | 563 MB | Joins `frames.preprocessed + tracking.cars + tracking.persons` by `(session_id, frame_number)`; draws colour boxes; publishes to `frames.rendered` |
| `web` | app | 8080 | 138 MB | Upload form; MJPEG `/stream/{session_id}`; `/stats` proxy |

Infra services (broker ×3, schema-registry, connect, ksqlDB, REST proxy, Prometheus, AlertManager, Control Center, Flink) are under the `infra` profile.

---

## Multi-session design

Every Kafka message carries `"session_id": "<uuid>"` as both a JSON field and the Kafka **message key**. Kafka routes by `hash(session_id) → partition`, so all messages for one session always land on the same partition — per-session ordering is guaranteed without coordination.

Stateful services maintain per-session state dicts:

- **Trackers** — `{session_id: CentroidTracker}`. A new tracker is created lazily on the first detection message for an unknown session. When `control.session_end` arrives, that tracker is removed.
- **Renderer** — `{(session_id, frame_number): {"frame", "cars", "persons"}}`. Renders when all three slots are filled; evicts on session end or buffer overflow.
- **Statistics** — `{session_id: Stats}` + global set of `(session_id, track_id)` pairs (track IDs reset to 0 per session, so raw IDs are only unique within a session).

---

## Neural Network

**YOLOv8n (Ultralytics, CPU-only PyTorch)**

- Pre-trained on COCO 80-class dataset — covers all required objects out of the box.
- Nano variant: ~3 MB weights, ~30–80 ms/frame on CPU.
- Installed as: `pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu` then `pip install ultralytics`. The CPU index must come first, otherwise pip resolves the GPU build (~1.5 GB extra).
- Car detector uses `CLASS_IDS=2,5,7`; person detector uses `CLASS_IDS=0` — same Docker image, different env var.

**Centroid tracking** (pure Python, `common/centroid_tracker.py`)

- `{track_id → centroid}` in memory per session.
- Greedy distance matching (threshold 100 px) + max-disappeared counter (30 frames).
- `total_seen == next_id` gives unique object count for the session.

---

## Kafka Topics

| Topic | Partitions | Max msg | Key | Notes |
|---|---|---|---|---|
| `frames.raw` | 3 | 5 MB | `session_id` | JPEG frames from generator |
| `frames.preprocessed` | 3 | 5 MB | `session_id` | Resized 640×640 |
| `frames.rendered` | 3 | 5 MB | `session_id` | Annotated JPEG from renderer |
| `detections.cars` | 3 | 256 KB | `session_id` | Car/bus/truck bboxes |
| `detections.persons` | 3 | 256 KB | `session_id` | Person bboxes |
| `tracking.cars` | 3 | 256 KB | `session_id` | Car tracks with unique IDs |
| `tracking.persons` | 3 | 256 KB | `session_id` | Person tracks with unique IDs |
| `control.upload` | 1 | 4 KB | `session_id` | `{session_id, file_path}` trigger |
| `control.session_end` | 1 | 4 KB | `session_id` | Emitted by generator when video is done |

---

## Message Schemas

**frames.raw / frames.preprocessed / frames.rendered**
```json
{"session_id": "uuid", "frame_number": 42, "timestamp": 1748452800.0,
 "width": 640, "height": 640, "data": "<base64 JPEG>"}
```

**detections.cars / detections.persons**
```json
{"session_id": "uuid", "frame_number": 42, "timestamp": 1748452800.0,
 "detections": [{"bbox": [x1,y1,x2,y2], "confidence": 0.87, "class_id": 2, "class_name": "car"}]}
```

**tracking.cars / tracking.persons**
```json
{"session_id": "uuid", "frame_number": 42, "object_type": "car",
 "tracks": [{"track_id": 3, "bbox": [x1,y1,x2,y2]}], "total_unique": 15}
```

**control.upload**
```json
{"session_id": "uuid", "file_path": "/uploads/uuid_filename.mp4"}
```

**control.session_end**
```json
{"session_id": "uuid", "total_frames": 704}
```

---

## Statistics API

`GET http://localhost:8002/stats` (also proxied via `GET http://localhost:8080/stats`)

```json
{
  "sessions": {
    "aaa-bbb": {"unique_cars": 5, "unique_persons": 12, "frames_cars": 704, "frames_persons": 704, "status": "done"},
    "ccc-ddd": {"unique_cars": 3, "unique_persons": 8,  "frames_cars": 210, "frames_persons": 210, "status": "processing"}
  },
  "global": {"unique_cars": 8, "unique_persons": 20}
}
```

Global counts deduplicate as `(session_id, track_id)` pairs because each session's `CentroidTracker` resets IDs from 0.

---

## How to Run

### Prerequisites

- Docker + Docker Compose v2
- ~8 GB free disk (detector images are ~1.9 GB each; two of them)
- ~4 GB RAM for all services

### Mode 1 — Full Docker (recommended)

```bash
cd capstone

# Step 1: start Kafka infrastructure
docker compose --profile infra up -d

# Step 2: build app images (first time only, ~5 min)
docker compose --profile app build

# Step 3: create topics (one-shot, idempotent)
docker compose --profile app up topic-init

# Step 4: start all app services
docker compose --profile app up -d --no-recreate

# Open the web UI
open http://localhost:8080
```

### Mode 2 — Hybrid (infra in Docker, apps run manually)

```bash
cd capstone
docker compose --profile infra up -d

python3.12 -m venv .venv && source .venv/bin/activate
pip install confluent-kafka opencv-python-headless fastapi uvicorn \
            python-multipart httpx jinja2
pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu
pip install ultralytics

export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export PYTHONPATH=services
mkdir -p /tmp/uploads

python services/topic_init/main.py

python services/generator/main.py &
python services/preprocessor/main.py &

CLASS_IDS=2,5,7 OUTPUT_TOPIC=detections.cars  GROUP_ID=detector-cars    python services/detector/main.py &
CLASS_IDS=0     OUTPUT_TOPIC=detections.persons GROUP_ID=detector-persons python services/detector/main.py &

OBJECT_TYPE=car    INPUT_TOPIC=detections.cars    OUTPUT_TOPIC=tracking.cars    python services/tracker/main.py &
OBJECT_TYPE=person INPUT_TOPIC=detections.persons OUTPUT_TOPIC=tracking.persons python services/tracker/main.py &

python services/statistics/main.py &
python services/renderer/main.py &

UPLOAD_DIR=/tmp/uploads python services/web/main.py
```

Then open `http://localhost:8080`.

---

## Environment Variables

All services read `KAFKA_BOOTSTRAP_SERVERS` (default: `broker:29092`; override to `localhost:9092` for hybrid).

| Service | Variable | Default | Description |
|---|---|---|---|
| generator | `CONTROL_TOPIC` | `control.upload` | Topic to listen for upload events |
| generator | `FRAME_INTERVAL` | `1` | Send every Nth frame |
| generator | `TARGET_WIDTH/HEIGHT` | `640/480` | Output frame resolution |
| preprocessor | `TARGET_WIDTH/HEIGHT` | `640/640` | YOLO input size |
| detector | `CLASS_IDS` | `2,5,7` | COCO class IDs to keep |
| detector | `CONFIDENCE` | `0.4` | Minimum detection confidence |
| tracker | `MAX_DISAPPEARED` | `30` | Frames before a track is dropped |
| tracker | `MAX_DISTANCE` | `100` | Pixel radius for centroid match |
| renderer | `MAX_BUFFER_ENTRIES` | `300` | Max `(session_id, frame_number)` entries before eviction |
| statistics | `HTTP_PORT` | `8002` | Port for the `/stats` HTTP endpoint |
| web | `UPLOAD_DIR` | `/uploads` | Where uploaded videos are saved |
| web | `STATS_URL` | `http://statistics:8002/stats` | Statistics service endpoint |
| web | `HTTP_PORT` | `8080` | Web server port |

---

## Verifying the Pipeline

```bash
# List all pipeline topics
docker exec broker kafka-topics --bootstrap-server broker:29092 --list

# Check consumer group lag (all groups should be active)
docker exec broker kafka-consumer-groups \
  --bootstrap-server broker:29092 --all-groups --describe

# Statistics HTTP API
curl http://localhost:8002/stats | python3 -m json.tool

# Web UI
open http://localhost:8080

# Service logs
docker compose --profile app logs -f renderer
docker compose --profile app logs -f statistics
```

---

## Directory Structure

```
capstone/
├── docker-compose.yaml          # infra (profile: infra) + 10 app services (profile: app)
├── input.mp4                    # sample video (can be uploaded via web UI too)
├── README.md
└── services/
    ├── common/
    │   ├── kafka_client.py      # producer/consumer factory + produce_with_backpressure()
    │   └── centroid_tracker.py  # per-session centroid tracker
    ├── generator/               # Dockerfile  main.py  requirements.txt
    ├── preprocessor/            # Dockerfile  main.py  requirements.txt
    ├── detector/                # shared image for car-detector + person-detector
    ├── tracker/                 # shared image for car-tracker + person-tracker
    ├── statistics/              # Dockerfile  main.py  requirements.txt
    ├── renderer/                # Dockerfile  main.py  requirements.txt
    ├── web/
    │   ├── Dockerfile
    │   ├── main.py
    │   ├── requirements.txt
    │   └── templates/
    │       ├── index.html       # upload form
    │       └── view.html        # MJPEG viewer + live stats
    └── topic_init/              # Dockerfile  main.py  requirements.txt
```
