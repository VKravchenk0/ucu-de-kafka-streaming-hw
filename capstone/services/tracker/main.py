"""Tracker: applies centroid tracking per session and publishes track events.

Each session_id gets its own CentroidTracker instance so multiple concurrent
video uploads do not interfere with each other.

Env vars:
  OBJECT_TYPE   "car" or "person"
  INPUT_TOPIC   (default: detections.cars)
  OUTPUT_TOPIC  (default: tracking.cars)
  GROUP_ID
  MAX_DISAPPEARED  frames before a track is dropped (default: 30)
  MAX_DISTANCE     pixel radius for centroid matching (default: 100)
"""

import json
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.kafka_client import make_producer, make_consumer, produce_with_backpressure
from common.centroid_tracker import CentroidTracker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OBJECT_TYPE = os.environ.get("OBJECT_TYPE", "car")
INPUT_TOPIC = os.environ.get("INPUT_TOPIC", f"detections.{OBJECT_TYPE}s")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", f"tracking.{OBJECT_TYPE}s")
GROUP_ID = os.environ.get("GROUP_ID", f"tracker-{OBJECT_TYPE}")
MAX_DISAPPEARED = int(os.environ.get("MAX_DISAPPEARED", "30"))
MAX_DISTANCE = int(os.environ.get("MAX_DISTANCE", "100"))
SESSION_END_TOPIC = "control.session_end"


def main() -> None:
    # session_id → CentroidTracker
    trackers: dict[str, CentroidTracker] = {}

    producer = make_producer()
    consumer = make_consumer(GROUP_ID, [INPUT_TOPIC, SESSION_END_TOPIC])
    processed = 0

    logger.info("Tracker ready — object_type=%s  %s → %s", OBJECT_TYPE, INPUT_TOPIC, OUTPUT_TOPIC)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            envelope = json.loads(msg.value())
            session_id = envelope["session_id"]

            if msg.topic() == SESSION_END_TOPIC:
                removed = trackers.pop(session_id, None)
                if removed is not None:
                    logger.info("[%s] Session ended — total_unique_%ss=%d",
                                session_id[:8], OBJECT_TYPE, removed.total_seen)
                continue

            # Lazily create tracker for new session
            if session_id not in trackers:
                trackers[session_id] = CentroidTracker(
                    max_disappeared=MAX_DISAPPEARED,
                    max_distance=MAX_DISTANCE,
                )

            tracker = trackers[session_id]
            active_tracks = tracker.update(envelope.get("detections", []))

            out = json.dumps({
                "session_id": session_id,
                "frame_number": envelope["frame_number"],
                "timestamp": envelope.get("timestamp"),
                "object_type": OBJECT_TYPE,
                "tracks": [{"track_id": tid, "bbox": bbox} for tid, bbox in active_tracks.items()],
                "total_unique": tracker.total_seen,
            })
            produce_with_backpressure(producer, OUTPUT_TOPIC, session_id, out)
            processed += 1

            if processed % 50 == 0:
                logger.info(
                    "Tracked %d frames — sessions=%d  active_tracks=%d",
                    processed, len(trackers), len(active_tracks),
                )

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        consumer.close()
        logger.info("Tracker stopped — processed %d frames  sessions=%d", processed, len(trackers))


if __name__ == "__main__":
    main()
