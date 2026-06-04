# Project Brief

## What It Is

A real-time video analytics pipeline built on Apache Kafka. The user uploads an MP4 video through a browser, the pipeline detects and tracks cars and people frame-by-frame using YOLOv8n, and the browser plays the video with live bounding-box overlays drawn on a `<canvas>` — while the video is still being processed.

## Primary Purpose

- Demonstrate an end-to-end event-driven architecture using Kafka as the backbone
- Apply stream processing concepts from the UCU Data Engineering course
- Show real-time object detection and tracking in a consumer-facing web interface

## Scope

- **Input**: MP4 video uploaded via browser
- **Processing**: frame extraction → preprocessing → YOLOv8n inference → centroid tracking
- **Output**: live bounding-box overlays on browser canvas + per-session object counts
- **Classes detected**: cars (COCO IDs 2, 5, 7) and persons (COCO ID 0)

## What It Is Not

- Not a production system (no auth, no multi-user isolation, no persistent storage beyond uploads volume)
- Not a batch processing system — the pipeline is streaming end-to-end
- Not dependent on a specific GPU — runs on CPU by default, GPU is optional

## Project Location

```
capstone/                        ← all work is here
├── services/                    ← all service source code
├── docker-compose.yaml          ← full stack definition
├── docker-compose.gpu.yaml      ← GPU overlay (NVIDIA only)
├── Makefile                     ← common dev commands
└── docs/memory/                 ← this memory bank
```

## Key Constraint

Docker build context is always `./services` (never a subdirectory). This is required because `COPY common ./common` in every Dockerfile copies the shared `common/` package from the build context root.
