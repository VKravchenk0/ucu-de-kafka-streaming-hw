import json
import logging
import os
import threading

from common.centroid_tracker import CentroidTracker
from common.kafka_client import make_consumer, make_producer, produce_with_backpressure

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
# OBJECT_TYPE: "car" or "person"
OBJECT_TYPE = os.environ["OBJECT_TYPE"]
INPUT_TOPIC = os.environ["INPUT_TOPIC"]     # detections.cars or detections.persons
OUTPUT_TOPIC = os.environ["OUTPUT_TOPIC"]   # tracking.cars or tracking.persons
GROUP_ID = os.environ["GROUP_ID"]           # tracker-car or tracker-person

MAX_DISAPPEARED = int(os.environ.get("MAX_DISAPPEARED", "30"))
MAX_DISTANCE = float(os.environ.get("MAX_DISTANCE", "100"))

# {session_id: CentroidTracker}
trackers: dict[str, CentroidTracker] = {}
# {session_id: set of seen track_ids} for per-session unique count
seen: dict[str, set[int]] = {}


def get_tracker(session_id: str) -> CentroidTracker:
    if session_id not in trackers:
        trackers[session_id] = CentroidTracker(
            max_disappeared=MAX_DISAPPEARED, max_distance=MAX_DISTANCE
        )
        seen[session_id] = set()
    return trackers[session_id]


def handle_detection(payload: dict, producer) -> None:
    session_id = payload["session_id"]
    tracker = get_tracker(session_id)

    bboxes = [d["bbox"] for d in payload.get("detections", [])]
    current_tracks = tracker.update(bboxes)

    for track_id in current_tracks:
        seen[session_id].add(track_id)

    tracks_list = [
        {"track_id": tid, "bbox": bbox}
        for tid, bbox in current_tracks.items()
    ]

    out = json.dumps({
        "session_id": session_id,
        "frame_number": payload["frame_number"],
        "video_timestamp_ms": payload["video_timestamp_ms"],
        "object_type": OBJECT_TYPE,
        "tracks": tracks_list,
        "in_frame": len(current_tracks),
        "total_unique": len(seen[session_id]),
    }).encode()

    produce_with_backpressure(producer, OUTPUT_TOPIC, session_id, out)


def handle_session_end(payload: dict) -> None:
    session_id = payload.get("session_id", "")
    total_frames = payload.get("total_frames", 0)
    # control.session_end arrives ~8s after upload; CPU detection takes ~total_frames/5 s.
    # Cleaning up immediately resets the seen set mid-stream → total_unique ≈ in_frame.
    delay = max(30.0, total_frames / 5.0)

    def _cleanup() -> None:
        trackers.pop(session_id, None)
        seen.pop(session_id, None)
        logger.info("session=%s  cleaned up tracker", session_id)

    t = threading.Timer(delay, _cleanup)
    t.daemon = True
    t.start()
    logger.info("session=%s  cleanup scheduled in %.0fs", session_id, delay)


def main() -> None:
    consumer = make_consumer(
        BOOTSTRAP,
        [INPUT_TOPIC, "control.session_end"],
        GROUP_ID,
    )
    producer = make_producer(BOOTSTRAP)
    logger.info("Tracker object_type=%s  input=%s  output=%s", OBJECT_TYPE, INPUT_TOPIC, OUTPUT_TOPIC)

    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        try:
            if msg.topic() == "control.session_end":
                handle_session_end(json.loads(msg.value()))
            else:
                handle_detection(json.loads(msg.value()), producer)
        except Exception as e:
            logger.error("Tracker error: %s", e)


if __name__ == "__main__":
    main()
