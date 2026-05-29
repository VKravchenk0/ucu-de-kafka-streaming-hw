"""Generator: reads control.upload messages, splits each video into frames,
publishes them to frames.raw keyed by session_id, then emits control.session_end.

Each upload is processed in its own thread so multiple concurrent sessions work.
"""

import base64
import json
import logging
import os
import sys
import threading
import time

import cv2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.kafka_client import make_producer, make_consumer, produce_with_backpressure

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "frames.raw")
CONTROL_TOPIC = os.environ.get("CONTROL_TOPIC", "control.upload")
SESSION_END_TOPIC = os.environ.get("SESSION_END_TOPIC", "control.session_end")
FRAME_INTERVAL = int(os.environ.get("FRAME_INTERVAL", "1"))
TARGET_WIDTH = int(os.environ.get("TARGET_WIDTH", "640"))
TARGET_HEIGHT = int(os.environ.get("TARGET_HEIGHT", "480"))
JPEG_QUALITY = int(os.environ.get("JPEG_QUALITY", "85"))


def process_session(producer, session_id: str, file_path: str) -> None:
    """Process one video file end-to-end in a dedicated thread."""
    logger.info("[%s] Starting — file=%s", session_id, file_path)

    cap = cv2.VideoCapture(file_path)
    if not cap.isOpened():
        logger.error("[%s] Cannot open video: %s", session_id, file_path)
        return

    fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
    total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    logger.info("[%s] fps=%.1f  total_frames=%d", session_id, fps, total)

    frame_number = 0
    sent = 0

    while True:
        ret, frame = cap.read()
        if not ret:
            break
        frame_number += 1
        if frame_number % FRAME_INTERVAL != 0:
            continue

        resized = cv2.resize(frame, (TARGET_WIDTH, TARGET_HEIGHT))
        ok, buf = cv2.imencode(".jpg", resized, [cv2.IMWRITE_JPEG_QUALITY, JPEG_QUALITY])
        if not ok:
            continue

        payload = json.dumps({
            "session_id": session_id,
            "frame_number": frame_number,
            "timestamp": time.time(),
            "width": TARGET_WIDTH,
            "height": TARGET_HEIGHT,
            "data": base64.b64encode(buf.tobytes()).decode("ascii"),
        })
        produce_with_backpressure(producer, OUTPUT_TOPIC, session_id, payload)
        sent += 1

        if sent % 100 == 0:
            logger.info("[%s] Sent %d frames", session_id, sent)

    cap.release()
    producer.flush()

    # Signal downstream services that this session is complete
    end_msg = json.dumps({"session_id": session_id, "total_frames": sent})
    produce_with_backpressure(producer, SESSION_END_TOPIC, session_id, end_msg)
    producer.flush()

    logger.info("[%s] Done — sent %d frames, session_end emitted", session_id, sent)


def main() -> None:
    producer = make_producer()
    consumer = make_consumer("generator-control-group", [CONTROL_TOPIC])
    logger.info("Generator ready — listening on %s", CONTROL_TOPIC)

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
            file_path = envelope["file_path"]

            t = threading.Thread(
                target=process_session,
                args=(producer, session_id, file_path),
                daemon=True,
                name=f"session-{session_id[:8]}",
            )
            t.start()

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        consumer.close()
        logger.info("Generator stopped")


if __name__ == "__main__":
    main()
