"""Tracker: applies centroid tracking to detections and publishes track events.

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


def main() -> None:
    tracker = CentroidTracker(max_disappeared=MAX_DISAPPEARED, max_distance=MAX_DISTANCE)
    producer = make_producer()
    consumer = make_consumer(GROUP_ID, [INPUT_TOPIC])
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
            active_tracks = tracker.update(envelope.get("detections", []))

            tracks_list = [
                {"track_id": tid, "bbox": bbox}
                for tid, bbox in active_tracks.items()
            ]

            out = json.dumps({
                "frame_number": envelope["frame_number"],
                "timestamp": envelope.get("timestamp"),
                "object_type": OBJECT_TYPE,
                "tracks": tracks_list,
                "total_unique": tracker.total_seen,
            })

            produce_with_backpressure(producer, OUTPUT_TOPIC, str(envelope["frame_number"]), out)
            processed += 1

            if processed % 50 == 0:
                logger.info(
                    "Tracked %d frames — active=%d  total_unique=%d",
                    processed, len(active_tracks), tracker.total_seen,
                )

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        consumer.close()
        logger.info(
            "Tracker stopped — processed %d frames  total_unique_%ss=%d",
            processed, OBJECT_TYPE, tracker.total_seen,
        )


if __name__ == "__main__":
    main()
