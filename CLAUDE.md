# CLAUDE.md — UCU Kafka Streaming Homework

## Memory Bank

This project has a persistent memory bank. **Always use it.**

**Location:** `/home/vs/.claude/projects/-home-vs-p-ucu-de-09-data-streaming-with-kafka-ucu-de-kafka-streaming-hw/memory/`

### At the start of every conversation

1. Read `MEMORY.md` (the index — always loaded automatically in context).
2. Read any memory files whose description matches the current task before writing code or giving advice.
3. Cross-check memory claims against current file state before asserting — memories can be stale.

### Update memory whenever

| Trigger | File to update |
|---|---|
| New bug found and fixed | `feedback_capstone.md` |
| Anti-pattern or pitfall encountered | `feedback_capstone.md` |
| User corrects your approach | `feedback_capstone.md` |
| Non-obvious approach confirmed working | `feedback_capstone.md` |
| Architecture decision or new service added | `project_capstone.md` |
| New Kafka topic, schema, or consumer group added | `project_capstone_topics.md` |
| User working style or preference observed | `user_profile.md` |
| External resource or doc location learned | create `reference_*.md` |

### Memory file format

```markdown
---
name: slug-in-kebab-case
description: one-line summary used to judge relevance
metadata:
  type: feedback | project | user | reference
---

Body. For feedback/project: lead with the rule/fact, then **Why:** and **How to apply:** lines.
Link related memories with [[their-name]].
```

Add a pointer line in `MEMORY.md`: `- [Title](file.md) — one-line hook`

---

## Project: Capstone — E2E Kafka Video Analytics

All capstone work lives in `capstone/`. See `capstone/README.md` for full architecture and quickstart.

Key facts to keep in mind:
- Docker build context is always `./services` (never a subdirectory) — required for `COPY common ./common`
- `torch`/`torchvision` must **never** appear in `requirements.txt` — install via explicit index URL in Dockerfile
- GPU support uses `PROCESSING_UNIT_TYPE=cuda` build arg + `docker-compose.gpu.yaml` overlay
- The `5` in `max(30, total_frames / DETECTION_FPS_ESTIMATE)` is CPU YOLOv8n throughput in fps — it's a named constant now, not a magic number
- Premature `control.session_end` is the root cause of any "stats reset mid-session" or "overlays stop early" bug — both web and tracker services delay their cleanup/done signal by `total_frames / DETECTION_FPS_ESTIMATE` seconds
