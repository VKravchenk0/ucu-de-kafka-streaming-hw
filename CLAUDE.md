# CLAUDE.md — UCU Kafka Streaming Homework

## Memory Systems

This project uses **two complementary memory stores**. Use both.

---

### 1. Video Pipeline Docs Memory Bank (primary project knowledge)

**Location:** `video-processing-pipeline/docs/memory/`

Seven structured markdown files that document the project itself — read these before writing code or giving architectural advice.

| File | Contains |
|---|---|
| `projectbrief.md` | What the project is, scope, key constraints |
| `productContext.md` | UX goals, user scenarios, badge states |
| `techContext.md` | Versions, env vars, Makefile commands |
| `systemPatterns.md` | Message flow, topic design, code patterns |
| `activeContext.md` | Current focus, recent changes, known issues |
| `progress.md` | Feature checklist, milestone status |
| `decisionLog.md` | Architectural decisions with rationale |

#### At the start of every conversation

1. Read `activeContext.md` first — it has the current focus and known issues.
2. Read any other files relevant to the task (e.g., `systemPatterns.md` before touching the pipeline, `techContext.md` before adding dependencies).
3. Cross-check claims against actual source files — docs can be stale.

#### Update these files whenever

| Trigger | File(s) to update |
|---|---|
| New feature completed or milestone reached | `progress.md`, `activeContext.md` |
| Architecture changes (new service, topic, pattern) | `systemPatterns.md`, `activeContext.md` |
| New dependency or version pin | `techContext.md` |
| Architectural decision made | `decisionLog.md` |
| Bug found and fixed (non-obvious root cause) | `decisionLog.md`, `activeContext.md` |
| Work shifts focus to a different area | `activeContext.md` |
| Known issue resolved | `progress.md`, `activeContext.md` |

**Keep each file under 200 lines.** Prefer updating existing entries over appending new ones.

---

### 2. Auto-Memory Bank (cross-conversation personal notes)

**Location:** `/home/vs/.claude/projects/-home-vs-p-ucu-de-09-data-streaming-with-kafka-ucu-de-kafka-streaming-hw/memory/`

Claude's personal notes — user preferences, feedback patterns, pitfalls encountered. `MEMORY.md` index is always loaded automatically in context.

#### Update auto-memory whenever

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

#### Auto-memory file format

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

## Project: Video Processing Pipeline — E2E Kafka Video Analytics

All work lives in `video-processing-pipeline/`. See `video-processing-pipeline/README.md` for full architecture and quickstart.

Key facts to keep in mind:
- Docker build context is always `./services` (never a subdirectory) — required for `COPY common ./common`
- `torch`/`torchvision` must **never** appear in `requirements.txt` — install via explicit index URL in Dockerfile
- GPU support uses `PROCESSING_UNIT_TYPE=cuda` build arg + `docker-compose.gpu.yaml` overlay
- The `5` in `max(30, total_frames / DETECTION_FPS_ESTIMATE)` is CPU YOLOv8n throughput in fps — it's a named constant now, not a magic number
- Premature `control.session_end` is the root cause of any "overlays stop early" bug — `web` delays its `_done` signal by `total_frames / DETECTION_FPS_ESTIMATE` seconds

## Formatting Rules

- **Mermaid diagrams**: use `<br/>` for line breaks inside node labels — never `\n`
