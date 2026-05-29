"""Statistics: aggregates unique car and person counts per session and globally.

Exposes GET /stats on port 8002 for the web service to proxy.
Track IDs restart at 0 for each session, so global uniqueness is tracked as
(session_id, track_id) pairs rather than raw track IDs.
"""

import json
import logging
import os
import sys
import threading
import time

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.kafka_client import make_consumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CAR_TOPIC = os.environ.get("CAR_TOPIC", "tracking.cars")
PERSON_TOPIC = os.environ.get("PERSON_TOPIC", "tracking.persons")
SESSION_END_TOPIC = "control.session_end"
GROUP_ID = os.environ.get("GROUP_ID", "statistics-group")
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8002"))


class SessionStats:
    def __init__(self):
        self.unique_cars: set[int] = set()
        self.unique_persons: set[int] = set()
        self.frames_cars: int = 0
        self.frames_persons: int = 0
        self.status: str = "processing"

    def to_dict(self) -> dict:
        return {
            "unique_cars": len(self.unique_cars),
            "unique_persons": len(self.unique_persons),
            "frames_cars": self.frames_cars,
            "frames_persons": self.frames_persons,
            "status": self.status,
        }


class GlobalAggregator:
    """Tracks uniqueness globally as (session_id, track_id) pairs."""
    def __init__(self):
        self._lock = threading.Lock()
        self._sessions: dict[str, SessionStats] = {}
        # global sets store (session_id, track_id) tuples to avoid ID collisions across sessions
        self._global_cars: set[tuple] = set()
        self._global_persons: set[tuple] = set()

    def update(self, msg: dict) -> None:
        session_id = msg.get("session_id", "unknown")
        obj_type = msg.get("object_type", "")
        track_ids = [t["track_id"] for t in msg.get("tracks", [])]

        with self._lock:
            if session_id not in self._sessions:
                self._sessions[session_id] = SessionStats()
            sess = self._sessions[session_id]

            if obj_type == "car":
                sess.unique_cars.update(track_ids)
                sess.frames_cars += 1
                self._global_cars.update((session_id, tid) for tid in track_ids)
            elif obj_type == "person":
                sess.unique_persons.update(track_ids)
                sess.frames_persons += 1
                self._global_persons.update((session_id, tid) for tid in track_ids)

    def mark_done(self, session_id: str) -> None:
        with self._lock:
            if session_id in self._sessions:
                self._sessions[session_id].status = "done"

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "sessions": {sid: s.to_dict() for sid, s in self._sessions.items()},
                "global": {
                    "unique_cars": len(self._global_cars),
                    "unique_persons": len(self._global_persons),
                },
            }


aggregator = GlobalAggregator()
app = FastAPI()


@app.get("/stats")
def get_stats():
    return JSONResponse(aggregator.snapshot())


def _print_loop(interval: float = 5.0) -> None:
    while True:
        time.sleep(interval)
        s = aggregator.snapshot()
        lines = ["\n" + "=" * 50, "  GLOBAL  unique cars: {unique_cars:>5}  people: {unique_persons:>5}".format(**s["global"])]
        for sid, ss in s["sessions"].items():
            lines.append(f"  [{sid[:8]}] cars={ss['unique_cars']}  people={ss['unique_persons']}  status={ss['status']}")
        lines.append("=" * 50)
        print("\n".join(lines), flush=True)


def _kafka_loop() -> None:
    consumer = make_consumer(GROUP_ID, [CAR_TOPIC, PERSON_TOPIC, SESSION_END_TOPIC])
    processed = 0
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            envelope = json.loads(msg.value())

            if msg.topic() == SESSION_END_TOPIC:
                aggregator.mark_done(envelope.get("session_id", ""))
            else:
                aggregator.update(envelope)
                processed += 1
    except Exception as exc:
        logger.error("Kafka loop crashed: %s", exc)
    finally:
        consumer.close()


def main() -> None:
    threading.Thread(target=_kafka_loop, daemon=True, name="kafka-consumer").start()
    threading.Thread(target=_print_loop, daemon=True, name="printer").start()
    logger.info("Statistics HTTP server on :%d", HTTP_PORT)
    uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT, log_level="warning")


if __name__ == "__main__":
    main()
