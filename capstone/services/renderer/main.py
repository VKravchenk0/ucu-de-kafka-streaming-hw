"""Renderer: joins frames.preprocessed + tracking.cars + tracking.persons by
(session_id, frame_number), draws bounding-box overlays with OpenCV, and
publishes annotated JPEG frames to frames.rendered.

Buffer management:
  - Entries expire when all three slots are filled (happy path).
  - Stale entries (only partial data) are evicted once the buffer exceeds
    MAX_BUFFER_ENTRIES, dropping the oldest frame_numbers per session.
  - On control.session_end all remaining entries for that session are dropped.
"""

import base64
import json
import logging
import os
import sys

import cv2
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.kafka_client import make_producer, make_consumer, produce_with_backpressure

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "frames.rendered")
GROUP_ID = os.environ.get("GROUP_ID", "renderer-group")
MAX_BUFFER_ENTRIES = int(os.environ.get("MAX_BUFFER_ENTRIES", "300"))

TOPICS = ["frames.preprocessed", "tracking.cars", "tracking.persons", "control.session_end"]

# Colours per object type (BGR)
COLOURS = {"car": (0, 60, 220), "person": (30, 180, 30)}
LABEL_BG = {"car": (0, 30, 160), "person": (10, 120, 10)}


def draw_overlays(data_b64: str, cars: list[dict], persons: list[dict]) -> str:
    raw = base64.b64decode(data_b64)
    arr = np.frombuffer(raw, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)

    for track_type, tracks in (("car", cars), ("person", persons)):
        colour = COLOURS[track_type]
        bg = LABEL_BG[track_type]
        for t in tracks:
            x1, y1, x2, y2 = [int(v) for v in t["bbox"]]
            tid = t["track_id"]
            label = f"{track_type[0].upper()}{tid}"  # e.g. "C3", "P7"

            cv2.rectangle(img, (x1, y1), (x2, y2), colour, 2)

            (tw, th), _ = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.55, 1)
            cv2.rectangle(img, (x1, y1 - th - 6), (x1 + tw + 4, y1), bg, -1)
            cv2.putText(img, label, (x1 + 2, y1 - 4),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.55, (255, 255, 255), 1, cv2.LINE_AA)

    _, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, 80])
    return base64.b64encode(buf.tobytes()).decode("ascii")


def evict_stale(buffer: dict, session_id: str | None = None) -> None:
    """Remove oldest entries when buffer is too large, or all entries for a session."""
    if session_id is not None:
        keys = [k for k in list(buffer) if k[0] == session_id]
        for k in keys:
            del buffer[k]
        return
    if len(buffer) > MAX_BUFFER_ENTRIES:
        # Sort by frame_number and drop the oldest 20%
        sorted_keys = sorted(buffer.keys(), key=lambda k: k[1])
        for k in sorted_keys[: len(sorted_keys) // 5]:
            del buffer[k]


def main() -> None:
    producer = make_producer()
    consumer = make_consumer(GROUP_ID, TOPICS)
    rendered = 0

    # buffer[(session_id, frame_number)] = {"frame": str|None, "cars": list|None, "persons": list|None}
    buffer: dict[tuple, dict] = {}

    logger.info("Renderer ready — consuming %s", TOPICS)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            envelope = json.loads(msg.value())
            session_id = envelope.get("session_id", "")
            topic = msg.topic()

            if topic == "control.session_end":
                evict_stale(buffer, session_id=session_id)
                logger.info("[%s] Session ended, buffer evicted", session_id[:8])
                continue

            frame_number = envelope.get("frame_number", 0)
            key = (session_id, frame_number)
            entry = buffer.setdefault(key, {"frame": None, "cars": None, "persons": None})

            if topic == "frames.preprocessed":
                entry["frame"] = envelope["data"]
            elif topic == "tracking.cars":
                entry["cars"] = envelope.get("tracks", [])
            elif topic == "tracking.persons":
                entry["persons"] = envelope.get("tracks", [])

            # Render when all three slots are filled
            if entry["frame"] is not None and entry["cars"] is not None and entry["persons"] is not None:
                annotated = draw_overlays(entry["frame"], entry["cars"], entry["persons"])
                out = json.dumps({
                    "session_id": session_id,
                    "frame_number": frame_number,
                    "data": annotated,
                })
                produce_with_backpressure(producer, OUTPUT_TOPIC, session_id, out)
                del buffer[key]
                rendered += 1
                if rendered % 50 == 0:
                    logger.info("Rendered %d frames, buffer_size=%d", rendered, len(buffer))

            evict_stale(buffer)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        consumer.close()
        logger.info("Renderer stopped — rendered %d frames", rendered)


if __name__ == "__main__":
    main()
