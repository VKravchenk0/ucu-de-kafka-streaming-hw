import asyncio
import json
import logging
import os
import threading
import uuid
from pathlib import Path

import httpx
import uvicorn
from fastapi import FastAPI, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request
import aiofiles

from common.kafka_client import make_consumer, make_producer, produce_with_backpressure

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
STATS_URL = os.environ.get("STATS_URL", "http://statistics:8002/stats")
UPLOAD_DIR = Path(os.environ.get("UPLOAD_DIR", "/uploads"))
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8080"))

# CPU YOLOv8n throughput on typical hardware.  Used to estimate how long the
# detection pipeline needs after the generator finishes emitting frames.
# formula: delay = max(MIN_DONE_DELAY_S, total_frames / DETECTION_FPS_ESTIMATE)
DETECTION_FPS_ESTIMATE: float = float(os.environ.get("DETECTION_FPS_ESTIMATE", "5"))
MIN_DONE_DELAY_S: float = 30.0
OVERLAY_BUFFER_DELAY_S: int = int(os.environ.get("OVERLAY_BUFFER_DELAY_S", "4"))

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# ---------------------------------------------------------------------------
# Durable per-session overlay store
# ---------------------------------------------------------------------------
# _overlay_store  — accumulates all overlay dicts as they arrive; never deleted
# _session_done   — True once control.session_end is received for that session
# _subscribers    — per-connection asyncio.Queue; one per active WebSocket
# All three are accessed from both the Kafka thread and async handlers → use locks.

_overlay_store: dict[str, list[dict]] = {}
_session_done:  dict[str, bool]       = {}
_subscribers:   dict[str, list[asyncio.Queue]] = {}

_store_lock       = threading.Lock()
_subscribers_lock = threading.Lock()

# The main event loop (set on startup, used for thread → async handoff)
_loop: asyncio.AbstractEventLoop | None = None


def _dispatch(session_id: str, item: dict) -> None:
    """Persist overlay to store and fan-out to every active WebSocket for this session."""
    with _store_lock:
        _overlay_store.setdefault(session_id, []).append(item)

    if _loop is None:
        return

    with _subscribers_lock:
        for q in _subscribers.get(session_id, []):
            if q.full():
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            asyncio.run_coroutine_threadsafe(q.put(item), _loop)


def _schedule_done_signal(session_id: str, total_frames: int) -> None:
    """Delay the _done signal so the detection pipeline has time to finish.

    control.session_end is produced by the generator when it finishes emitting frames
    (~8 s), but CPU detection runs at ~5 fps so a 695-frame video takes ~139 s to fully
    process.  Dispatching _done immediately closes the WebSocket before all tracking
    messages arrive.  We schedule the dispatch after max(30, total_frames / 5) seconds.
    """
    delay = max(MIN_DONE_DELAY_S, total_frames / DETECTION_FPS_ESTIMATE)

    def _send_done() -> None:
        done_item = {"_done": True, "total_frames": total_frames}
        with _store_lock:
            _session_done[session_id] = True
        if _loop is None:
            return
        with _subscribers_lock:
            for q in _subscribers.get(session_id, []):
                if q.full():
                    try:
                        q.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                asyncio.run_coroutine_threadsafe(q.put(done_item), _loop)
        logger.info("session=%s  _done dispatched  total_frames=%d", session_id, total_frames)

    t = threading.Timer(delay, _send_done)
    t.daemon = True
    t.start()
    logger.info("session=%s  _done scheduled in %.0fs  total_frames=%d", session_id, delay, total_frames)


# ---------------------------------------------------------------------------
# Kafka consumer thread
# ---------------------------------------------------------------------------

def kafka_consumer_thread() -> None:
    # Consume tracking.cars and tracking.persons directly — bypasses ksqlDB windowed
    # join delays that caused most frames to be absent from tracking.combined.
    # ksqlDB / tracking.combined is still consumed by the statistics service.
    consumer = make_consumer(
        BOOTSTRAP,
        ["tracking.cars", "tracking.persons", "control.session_end"],
        "web-overlay-group",
    )
    logger.info("Web Kafka consumer started (direct tracking topics)")

    # Merge buffer: session_id → {video_timestamp_ms → combined overlay dict}
    # Cars and persons for the same frame update the same entry; dispatch on each update.
    merge_buf: dict[str, dict[float, dict]] = {}

    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        try:
            topic = msg.topic()
            payload = json.loads(msg.value())
            session_id = payload.get("session_id", "")

            if topic == "control.session_end":
                _schedule_done_signal(session_id, payload.get("total_frames", 0))
                merge_buf.pop(session_id, None)
                continue

            ts = payload.get("video_timestamp_ms", 0.0)

            if session_id not in merge_buf:
                merge_buf[session_id] = {}
            if ts not in merge_buf[session_id]:
                merge_buf[session_id][ts] = {
                    "session_id": session_id,
                    "video_timestamp_ms": ts,
                    "frame_number": payload.get("frame_number", 0),
                    "car_tracks": [],
                    "cars_in_frame": 0,
                    "cars_total": 0,
                    "person_tracks": [],
                    "persons_in_frame": 0,
                    "persons_total": 0,
                }

            entry = merge_buf[session_id][ts]
            if topic == "tracking.cars":
                entry["car_tracks"] = payload.get("tracks", [])
                entry["cars_in_frame"] = payload.get("in_frame", 0)
                entry["cars_total"] = payload.get("total_unique", 0)
            else:
                entry["person_tracks"] = payload.get("tracks", [])
                entry["persons_in_frame"] = payload.get("in_frame", 0)
                entry["persons_total"] = payload.get("total_unique", 0)

            _dispatch(session_id, dict(entry))

            # Cap memory: evict oldest entries beyond 10 000 per session
            if len(merge_buf[session_id]) > 10_000:
                oldest = min(merge_buf[session_id])
                del merge_buf[session_id][oldest]

        except Exception as e:
            logger.error("Web consumer error: %s", e)


# ---------------------------------------------------------------------------
# FastAPI lifecycle
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup() -> None:
    global _loop, _producer
    _loop = asyncio.get_event_loop()
    _producer = make_producer(BOOTSTRAP)
    t = threading.Thread(target=kafka_consumer_thread, daemon=True)
    t.start()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/upload")
async def upload(file: UploadFile):
    session_id = str(uuid.uuid4())
    dest = UPLOAD_DIR / f"{session_id}.mp4"

    async with aiofiles.open(dest, "wb") as f:
        while chunk := await file.read(1024 * 1024):
            await f.write(chunk)

    msg = json.dumps({"session_id": session_id, "file_path": str(dest)}).encode()
    produce_with_backpressure(_producer, "control.upload", session_id, msg)
    _producer.flush()
    logger.info("Uploaded session=%s  file=%s", session_id, dest)

    return RedirectResponse(f"/view/{session_id}", status_code=303)


@app.get("/video/{session_id}")
async def serve_video(session_id: str):
    path = UPLOAD_DIR / f"{session_id}.mp4"
    if not path.exists():
        return HTMLResponse("Not found", status_code=404)
    return FileResponse(str(path), media_type="video/mp4")


@app.get("/view/{session_id}", response_class=HTMLResponse)
async def view(request: Request, session_id: str):
    return templates.TemplateResponse("view.html", {
        "request": request,
        "session_id": session_id,
        "buffer_delay_s": OVERLAY_BUFFER_DELAY_S,
    })


@app.websocket("/ws/{session_id}")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    await websocket.accept()

    # Create a per-connection subscriber queue and register it BEFORE reading the
    # store, so no items can slip through the gap between the two operations.
    sub: asyncio.Queue = asyncio.Queue(maxsize=10_000)
    with _subscribers_lock:
        _subscribers.setdefault(session_id, []).append(sub)

    try:
        # Phase 1: catch-up — replay everything received so far for this session.
        # This makes page-reload and multi-user scenarios work correctly.
        with _store_lock:
            existing = list(_overlay_store.get(session_id, []))
            done     = _session_done.get(session_id, False)

        for item in existing:
            await websocket.send_text(json.dumps(item))

        if done:
            await websocket.send_text(json.dumps({"_done": True}))
            return

        # Phase 2: live stream — drain subscriber queue as new overlays arrive.
        while True:
            item = await asyncio.wait_for(sub.get(), timeout=300.0)
            await websocket.send_text(json.dumps(item))
            if item.get("_done"):
                break

    except (WebSocketDisconnect, asyncio.TimeoutError):
        pass
    finally:
        # Remove only the subscriber queue; the store is kept for future connections.
        with _subscribers_lock:
            subs = _subscribers.get(session_id, [])
            if sub in subs:
                subs.remove(sub)
        logger.info("WebSocket closed  session=%s  stored=%d",
                    session_id, len(_overlay_store.get(session_id, [])))


@app.get("/stats")
async def stats():
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(STATS_URL, timeout=5.0)
            return r.json()
        except Exception as e:
            return {"error": str(e)}


def main() -> None:
    uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT, log_level="info")


if __name__ == "__main__":
    main()
