import json
import logging
import os
import threading
from dataclasses import dataclass, field

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from common.kafka_client import make_consumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8002"))

app = FastAPI()

# Per-session stats
@dataclass
class SessionStats:
    car_ids: set[int] = field(default_factory=set)
    person_ids: set[int] = field(default_factory=set)
    status: str = "processing"


_lock = threading.Lock()
sessions: dict[str, SessionStats] = {}
# Global unique counts use (session_id, track_id) tuples — track IDs restart per session
global_cars: set[tuple[str, int]] = set()
global_persons: set[tuple[str, int]] = set()


def get_session(session_id: str) -> SessionStats:
    if session_id not in sessions:
        sessions[session_id] = SessionStats()
    return sessions[session_id]


def _lower(obj):
    """Recursively lowercase all dict keys (ksqlDB outputs UPPERCASE field names)."""
    if isinstance(obj, dict):
        return {k.lower(): _lower(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_lower(i) for i in obj]
    return obj


def handle_combined(session_id: str, payload: dict) -> None:
    with _lock:
        s = get_session(session_id)
        for t in payload.get("car_tracks") or []:
            tid = t["track_id"]
            s.car_ids.add(tid)
            global_cars.add((session_id, tid))
        for t in payload.get("person_tracks") or []:
            tid = t["track_id"]
            s.person_ids.add(tid)
            global_persons.add((session_id, tid))


def handle_session_end(payload: dict) -> None:
    session_id = payload.get("session_id", "")
    with _lock:
        s = get_session(session_id)
        s.status = "done"
    logger.info("session=%s  marked done", session_id)


def kafka_thread() -> None:
    consumer = make_consumer(
        BOOTSTRAP,
        ["tracking.combined", "control.session_end"],
        "statistics-group",
    )
    logger.info("Statistics consumer started")
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        try:
            if msg.topic() == "control.session_end":
                handle_session_end(json.loads(msg.value()))
            else:
                # ksqlDB uppercases all field names; session_id is in the message key
                payload = _lower(json.loads(msg.value()))
                session_id = msg.key().decode() if msg.key() else payload.get("session_id", "")
                handle_combined(session_id, payload)
        except Exception as e:
            logger.error("Statistics error: %s", e)


@app.get("/stats")
def get_stats() -> JSONResponse:
    with _lock:
        result = {
            "sessions": {
                sid: {
                    "unique_cars": len(s.car_ids),
                    "unique_persons": len(s.person_ids),
                    "status": s.status,
                }
                for sid, s in sessions.items()
            },
            "global": {
                "unique_cars": len(global_cars),
                "unique_persons": len(global_persons),
            },
        }
    return JSONResponse(result)


def main() -> None:
    t = threading.Thread(target=kafka_thread, daemon=True)
    t.start()
    uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT, log_level="warning")


if __name__ == "__main__":
    main()
