# Tech Context

## Runtime Versions (pinned in requirements.txt)

| Package | Version |
|---|---|
| Python | 3.12 (base image: python:3.12-slim) |
| confluent-kafka | 2.6.1 |
| opencv-python-headless | 4.10.0.84 |
| numpy | 2.1.3 |
| ultralytics (YOLOv8) | 8.3.52 |
| torch / torchvision | CPU or CUDA 12.1 (installed in detector Dockerfile, NOT in requirements.txt) |
| fastapi | 0.115.5 |
| uvicorn | 0.32.1 |
| jinja2 | 3.1.4 |
| httpx | 0.27.2 |
| aiofiles | 24.1.0 |
| python-multipart | 0.0.17 |

## Infrastructure (docker-compose.yaml, profile: infra)

| Service | Image | Purpose |
|---|---|---|
| broker | confluentinc/cp-kafka:7.5.0 | 3-broker KRaft Kafka cluster |
| schema-registry | confluentinc/cp-schema-registry:7.5.0 | Schema registry |
| connect | confluentinc/cp-kafka-connect:7.5.0 | Kafka Connect |
| control-center | confluentinc/cp-enterprise-control-center:7.5.0 | Kafka UI (port 9021) |
| rest-proxy | confluentinc/cp-kafka-rest:7.5.0 | REST proxy |
| prometheus | prom/prometheus:v2.54.1 | Metrics scraping |
| alertmanager | prom/alertmanager:v0.27.0 | Alerting |

## Application Services (docker-compose.yaml, profile: app)

| Container | Build | Port |
|---|---|---|
| topic-init | services/ | — |
| generator | services/ | — |
| preprocessor | services/ | — |
| car-detector | services/ (PROCESSING_UNIT_TYPE arg) | — |
| person-detector | services/ (PROCESSING_UNIT_TYPE arg) | — |
| tracking-streams | services/ (Java / Kafka Streams) | — |
| statistics | services/ | 8002 |
| web | services/ | 8080 |

## Common Makefile Commands

```bash
# Infrastructure
make infra-up        # start Kafka + monitoring stack
make infra-down      # stop infra

# CPU workflow (default)
make build-cpu       # build all app images (*:cpu tags)
make up-cpu          # start app stack with CPU detector images

# GPU workflow (NVIDIA)
make build-gpu       # build all app images (*:cuda tags)
make up-gpu          # start app stack with CUDA detector images

# Operations
make down            # stop app stack
make logs            # follow app logs
```

## GPU Build Details

```bash
# Produces: car-detector:cuda, person-detector:cuda
PROCESSING_UNIT_TYPE=cuda docker compose \
  -f docker-compose.yaml -f docker-compose.gpu.yaml \
  --profile app build
```

- `torch` installed from `https://download.pytorch.org/whl/cu121` for CUDA builds
- `torch` installed from `https://download.pytorch.org/whl/cpu` for CPU builds
- **torch must never appear in `requirements.txt`** — pip would silently pull the wrong build

## Key Environment Variables

| Variable | Default | Scope |
|---|---|---|
| KAFKA_BOOTSTRAP_SERVERS | (required) | all services |
| FRAME_INTERVAL | 1 | generator (set to 3 in docker-compose) |
| JPEG_QUALITY | 85 | generator, preprocessor |
| TARGET_SIZE | 640 | preprocessor |
| CLASS_IDS | (required) | detector (2,5,7 or 0) |
| CONF_THRESHOLD | 0.4 | detector |
| PROCESSING_UNIT_TYPE | cpu | detector (build + runtime) |
| DETECTION_FPS_ESTIMATE | 5 | web |
| OVERLAY_BUFFER_DELAY_S | 4 | web |
| HTTP_PORT | 8080 / 8002 | web, statistics |
| UPLOAD_DIR | /uploads | web |
