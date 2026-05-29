"""Web service: upload form, live MJPEG stream per session, stats proxy.

Endpoints:
  GET  /                    — upload form
  POST /upload              — save file, publish to control.upload, return session_id
  GET  /view/{session_id}   — viewer page (MJPEG + live stats)
  GET  /stream/{session_id} — MJPEG multipart stream (consumed by <img> tag)
  GET  /stats               — proxies GET http://statistics:8002/stats
"""

import asyncio
import base64
import json
import logging
import os
import sys
import threading
import uuid

import httpx
import uvicorn
from fastapi import FastAPI, File, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.kafka_client import make_producer, make_consumer, produce_with_backpressure

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

UPLOAD_DIR = os.environ.get("UPLOAD_DIR", "/uploads")
KAFKA_UPLOAD_TOPIC = os.environ.get("KAFKA_UPLOAD_TOPIC", "control.upload")
RENDERED_TOPIC = os.environ.get("RENDERED_TOPIC", "frames.rendered")
STATS_URL = os.environ.get("STATS_URL", "http://statistics:8002/stats")
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8080"))

os.makedirs(UPLOAD_DIR, exist_ok=True)

app = FastAPI()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

# session_id → asyncio.Queue[bytes]  (JPEG frame bytes)
_frame_queues: dict[str, asyncio.Queue] = {}
_event_loop: asyncio.AbstractEventLoop | None = None
_producer = None


# ── background Kafka consumer for rendered frames ────────────────────────────

def _kafka_rendered_loop() -> None:
    consumer = make_consumer("web-stream-group", [RENDERED_TOPIC])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.warning("Rendered consumer error: %s", msg.error())
            continue
        try:
            envelope = json.loads(msg.value())
            sid = envelope.get("session_id", "")
            q = _frame_queues.get(sid)
            if q and _event_loop:
                frame_bytes = base64.b64decode(envelope["data"])
                asyncio.run_coroutine_threadsafe(_put_frame(q, frame_bytes), _event_loop)
        except Exception as exc:
            logger.warning("Frame dispatch error: %s", exc)


async def _put_frame(q: asyncio.Queue, frame: bytes) -> None:
    if q.full():
        try:
            q.get_nowait()  # drop the oldest frame so the stream stays live
        except asyncio.QueueEmpty:
            pass
    await q.put(frame)


# ── FastAPI lifecycle ─────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup() -> None:
    global _event_loop, _producer
    _event_loop = asyncio.get_event_loop()
    _producer = make_producer()
    threading.Thread(target=_kafka_rendered_loop, daemon=True, name="kafka-rendered").start()
    logger.info("Web service ready on :%d", HTTP_PORT)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    session_id = str(uuid.uuid4())
    safe_name = "".join(c for c in file.filename if c.isalnum() or c in "._-")
    file_path = os.path.join(UPLOAD_DIR, f"{session_id}_{safe_name}")

    contents = await file.read()
    with open(file_path, "wb") as f:
        f.write(contents)

    msg = json.dumps({"session_id": session_id, "file_path": file_path})
    produce_with_backpressure(_producer, KAFKA_UPLOAD_TOPIC, session_id, msg)
    _producer.flush()

    logger.info("Upload saved: session=%s  file=%s  size=%d", session_id[:8], safe_name, len(contents))
    return JSONResponse({"session_id": session_id})


@app.get("/view/{session_id}", response_class=HTMLResponse)
async def view(request: Request, session_id: str):
    return templates.TemplateResponse("view.html", {"request": request, "session_id": session_id})


@app.get("/stream/{session_id}")
async def stream(session_id: str):
    if session_id not in _frame_queues:
        _frame_queues[session_id] = asyncio.Queue(maxsize=60)

    async def generate():
        q = _frame_queues[session_id]
        while True:
            try:
                frame = await asyncio.wait_for(q.get(), timeout=30.0)
                yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n\r\n" + frame + b"\r\n"
                )
            except asyncio.TimeoutError:
                # Send a keep-alive comment so the browser does not close the connection
                yield b"--frame\r\n\r\n"

    return StreamingResponse(generate(), media_type="multipart/x-mixed-replace; boundary=frame")


@app.get("/stats")
async def stats():
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(STATS_URL)
            return JSONResponse(resp.json())
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=503)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT, log_level="info")
