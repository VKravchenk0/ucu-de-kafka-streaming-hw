import json
import logging
import os
import threading

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from common.kafka_client import make_consumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8002"))

# {session_id: {"unique_cars": int, "unique_persons": int}}
_sessions: dict[str, dict] = {}
_lock = threading.Lock()

app = FastAPI()


@app.get("/stats")
def get_stats() -> JSONResponse:
    with _lock:
        return JSONResponse({"sessions": dict(_sessions)})


def kafka_consumer_thread() -> None:
    consumer = make_consumer(BOOTSTRAP, ["tracking.combined"], "statistics-group")
    logger.info("Statistics consumer started (tracking.combined)")

    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        try:
            session_id = msg.key().decode() if msg.key() else ""
            payload = json.loads(msg.value())
            with _lock:
                prev = _sessions.get(session_id, {"unique_cars": 0, "unique_persons": 0})
                # LEFT JOIN means persons_total can be None on a given record when the
                # persons side arrived outside the join window — keep the last non-null value.
                _sessions[session_id] = {
                    "unique_cars": payload.get("cars_total") or prev["unique_cars"],
                    "unique_persons": payload.get("persons_total") or prev["unique_persons"],
                }
        except Exception as e:
            logger.error("Statistics consumer error: %s", e)


def main() -> None:
    t = threading.Thread(target=kafka_consumer_thread, daemon=True)
    t.start()
    uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT, log_level="warning")


if __name__ == "__main__":
    main()
