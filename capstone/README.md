# Capstone: E2E Video Stream Processing Pipeline

End-to-end Kafka-based video pipeline that reads a video file, splits it into frames, detects and tracks cars and people using YOLOv8, and reports unique-object statistics.

---

## Architecture

```
input.mp4 (70 MB)
    │
    ▼
┌─────────────┐   topic: frames.raw  (3 partitions, 5 MB max msg)
│  generator  │ ────────────────────────────────────────────────► ┐
│             │   {frame_number, timestamp, width, height, data}  │
└─────────────┘   data = base64-encoded JPEG @ 640×480            │
                                                                    │
                ┌───────────────────────────────────────────────────┘
                ▼
        ┌──────────────┐   topic: frames.preprocessed  (3 partitions, 5 MB)
        │ preprocessor │ ────────────────────────────────┐
        │  resize 640² │                                 │
        └──────────────┘                                 │
                                           ┌─────────────┴──────────────┐
                                           ▼                            ▼
                                ┌──────────────────┐       ┌───────────────────────┐
                                │  car-detector    │       │  person-detector      │
                                │  YOLOv8n (CPU)   │       │  YOLOv8n (CPU)        │
                                │  class 2,5,7     │       │  class 0 (person)     │
                                └────────┬─────────┘       └──────────┬────────────┘
                                         │                             │
                                         ▼                             ▼
                               topic: detections.cars      topic: detections.persons
                               {frame_number, detections   (same schema)
                                [{bbox,conf,class_name}]}
                                         │                             │
                                         ▼                             ▼
                                ┌────────────────┐         ┌──────────────────┐
                                │  car-tracker   │         │  person-tracker  │
                                │  centroid alg. │         │  centroid alg.   │
                                └────────┬───────┘         └────────┬─────────┘
                                         │                           │
                                         ▼                           ▼
                               topic: tracking.cars        topic: tracking.persons
                               {frame_number, tracks       (same schema)
                                [{track_id, bbox}],
                                total_unique}
                                         │                           │
                                         └─────────────┬─────────────┘
                                                       ▼
                                             ┌──────────────────┐
                                             │   statistics     │
                                             │  Unique cars: N  │
                                             │  Unique people: M│
                                             └──────────────────┘
```

---

## Services

| Service | Profile | Image Size | Description |
|---|---|---|---|
| `topic-init` | app | 138 MB | One-shot: creates all 6 Kafka topics, then exits |
| `generator` | app | 563 MB | Reads `input.mp4`, encodes frames as JPEG, publishes to `frames.raw` |
| `preprocessor` | app | 563 MB | Resizes frames to 640×640, forwards to `frames.preprocessed` |
| `car-detector` | app | 1.9 GB | YOLOv8n, filters COCO classes {2=car, 5=bus, 7=truck} |
| `person-detector` | app | 1.9 GB | YOLOv8n (same image), filters COCO class {0=person} |
| `car-tracker` | app | 138 MB | Centroid tracker, assigns stable unique IDs to car tracks |
| `person-tracker` | app | 138 MB | Centroid tracker (same image), assigns stable unique IDs to persons |
| `statistics` | app | 138 MB | Aggregates tracking topics, prints unique counts every 5 s |

Infra services (broker ×3, schema-registry, connect, ksqlDB, REST proxy, Prometheus, AlertManager, Control Center, Flink) are all under the `infra` profile and defined in the existing `docker-compose.yaml`.

---

## Neural Network Decision

**YOLOv8n (Ultralytics, CPU-only)**

- Pre-trained on COCO 80-class dataset — covers all required objects out of the box
- Nano variant: ~3 MB weights, ~30–80 ms/frame on CPU
- Single `ultralytics` pip package; weights baked into the Docker image at build time
- Car detector uses `CLASS_IDS=2,5,7`; person detector uses `CLASS_IDS=0` — same image, different env var

**Centroid tracking** (pure Python, no extra ML model)

- Maintains `{track_id → centroid}` state in memory per service instance
- Greedy distance matching (threshold 100 px) + max-disappeared counter (30 frames)
- `total_seen` counter gives the running count of unique objects

---

## Kafka Topics

| Topic | Partitions | Max msg | Notes |
|---|---|---|---|
| `frames.raw` | 3 | 5 MB | Raw JPEG frames from generator |
| `frames.preprocessed` | 3 | 5 MB | Resized 640×640 frames |
| `detections.cars` | 3 | 256 KB | Car/bus/truck bboxes per frame |
| `detections.persons` | 3 | 256 KB | Person bboxes per frame |
| `tracking.cars` | 1 | 256 KB | Car tracks with unique IDs (single partition preserves order) |
| `tracking.persons` | 1 | 256 KB | Person tracks with unique IDs |

---

## Message Schemas

**frames.raw / frames.preprocessed**
```json
{
  "frame_number": 42,
  "timestamp": 1748452800.0,
  "width": 640,
  "height": 640,
  "data": "<base64-encoded JPEG>"
}
```

**detections.cars / detections.persons**
```json
{
  "frame_number": 42,
  "timestamp": 1748452800.0,
  "detections": [
    {"bbox": [x1, y1, x2, y2], "confidence": 0.87, "class_id": 2, "class_name": "car"}
  ]
}
```

**tracking.cars / tracking.persons**
```json
{
  "frame_number": 42,
  "timestamp": 1748452800.0,
  "object_type": "car",
  "tracks": [{"track_id": 3, "bbox": [x1, y1, x2, y2]}],
  "total_unique": 15
}
```

---

## How to Run

### Prerequisites

- Docker + Docker Compose v2
- ~6 GB free disk (detector images are ~1.9 GB each)
- ~4 GB RAM for all services

### Mode 1 — Full Docker (recommended)

```bash
cd capstone

# Step 1: start Kafka infrastructure
docker compose --profile infra up -d

# Step 2: build app images (first time only, ~5 min due to PyTorch download)
docker compose --profile app build

# Step 3: create topics (one-shot, exits when done)
docker compose --profile app up topic-init

# Step 4: start all app services
docker compose --profile app up -d --no-recreate

# Watch statistics
docker compose --profile app logs -f statistics

# Stop everything
docker compose --profile app down
docker compose --profile infra down
```

### Mode 2 — Hybrid (infra in Docker, apps run manually)

```bash
cd capstone

# Start infra
docker compose --profile infra up -d

# Install dependencies (use a virtualenv)
python3.12 -m venv .venv && source .venv/bin/activate
pip install confluent-kafka opencv-python-headless
pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu
pip install ultralytics

export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export PYTHONPATH=services

# Create topics
python services/topic_init/main.py

# Start each service in its own terminal (or background)
python services/generator/main.py &
python services/preprocessor/main.py &
CLASS_IDS=2,5,7 OUTPUT_TOPIC=detections.cars  GROUP_ID=detector-cars    python services/detector/main.py &
CLASS_IDS=0     OUTPUT_TOPIC=detections.persons GROUP_ID=detector-persons python services/detector/main.py &
OBJECT_TYPE=car    INPUT_TOPIC=detections.cars    OUTPUT_TOPIC=tracking.cars    python services/tracker/main.py &
OBJECT_TYPE=person INPUT_TOPIC=detections.persons OUTPUT_TOPIC=tracking.persons python services/tracker/main.py &
python services/statistics/main.py
```

---

## Environment Variables

All services read `KAFKA_BOOTSTRAP_SERVERS` (default: `broker:29092` inside Docker; override to `localhost:9092` for hybrid mode).

| Service | Variable | Default | Description |
|---|---|---|---|
| generator | `VIDEO_PATH` | `/data/input.mp4` | Path to input video |
| generator | `FRAME_INTERVAL` | `1` | Send every Nth frame |
| generator | `TARGET_WIDTH/HEIGHT` | `640/480` | Output frame size |
| generator | `LOOP` | `true` | Replay video after reaching end |
| preprocessor | `TARGET_WIDTH/HEIGHT` | `640/640` | YOLO input size |
| detector | `CLASS_IDS` | `2,5,7` | COCO class IDs to keep |
| detector | `CONFIDENCE` | `0.4` | Minimum detection confidence |
| tracker | `OBJECT_TYPE` | `car` | Label embedded in output messages |
| tracker | `MAX_DISAPPEARED` | `30` | Frames before track is dropped |
| tracker | `MAX_DISTANCE` | `100` | Pixel radius for centroid match |

---

## Verifying the Pipeline

```bash
# List all pipeline topics
docker exec broker kafka-topics --bootstrap-server broker:29092 --list

# Check consumer lag for all groups
docker exec broker kafka-consumer-groups \
  --bootstrap-server broker:29092 --all-groups --describe

# Tail statistics output
docker compose --profile app logs -f statistics

# Expected output (updates every 5 s):
# ============================================
#   Unique Cars:        19
#   Unique People:      45
#   Frames (cars):     308
#   Frames (people):   350
# ============================================
```

---

## Directory Structure

```
capstone/
├── docker-compose.yaml       # infra (profile: infra) + app services (profile: app)
├── input.mp4                 # 70 MB input video (cars + people)
├── README.md
└── services/
    ├── common/
    │   ├── kafka_client.py   # producer/consumer factory with retry + backpressure
    │   └── centroid_tracker.py
    ├── generator/            Dockerfile  main.py  requirements.txt
    ├── preprocessor/         Dockerfile  main.py  requirements.txt
    ├── detector/             Dockerfile  main.py  requirements.txt  (shared: car + person)
    ├── tracker/              Dockerfile  main.py  requirements.txt  (shared: car + person)
    ├── statistics/           Dockerfile  main.py  requirements.txt
    └── topic_init/           Dockerfile  main.py  requirements.txt
```
