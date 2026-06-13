import base64
import json
import logging
import os

import cv2
import numpy as np

from common.kafka_client import make_consumer, make_producer, produce_with_backpressure

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TARGET_SIZE = int(os.environ.get("TARGET_SIZE", "640"))
JPEG_QUALITY = int(os.environ.get("JPEG_QUALITY", "85"))


def process(msg_value: bytes, producer) -> None:
    payload = json.loads(msg_value)
    raw = base64.b64decode(payload["data"])
    arr = np.frombuffer(raw, dtype=np.uint8)
    frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if frame is None:
        return

    resized = cv2.resize(frame, (TARGET_SIZE, TARGET_SIZE))
    ok, buf = cv2.imencode(".jpg", resized, [cv2.IMWRITE_JPEG_QUALITY, JPEG_QUALITY])
    if not ok:
        return

    out = {
        "session_id": payload["session_id"],
        "frame_number": payload["frame_number"],
        "video_timestamp_ms": payload["video_timestamp_ms"],
        "width": TARGET_SIZE,
        "height": TARGET_SIZE,
        "data": base64.b64encode(buf.tobytes()).decode(),
    }
    produce_with_backpressure(
        producer, "frames.preprocessed", payload["session_id"], json.dumps(out).encode()
    )


def main() -> None:
    consumer = make_consumer(BOOTSTRAP, ["frames.raw"], "preprocessor-group")
    producer = make_producer(BOOTSTRAP)
    logger.info("Preprocessor running (target=%dx%d)", TARGET_SIZE, TARGET_SIZE)

    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        try:
            process(msg.value(), producer)
        except Exception as e:
            logger.error("Preprocessor error: %s", e)


if __name__ == "__main__":
    main()
