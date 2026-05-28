"""Preprocessor: resizes/normalises frames from frames.raw → frames.preprocessed."""

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

INPUT_TOPIC = os.environ.get("INPUT_TOPIC", "frames.raw")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "frames.preprocessed")
GROUP_ID = os.environ.get("GROUP_ID", "preprocessor-group")
TARGET_WIDTH = int(os.environ.get("TARGET_WIDTH", "640"))
TARGET_HEIGHT = int(os.environ.get("TARGET_HEIGHT", "640"))
JPEG_QUALITY = int(os.environ.get("JPEG_QUALITY", "85"))


def preprocess(data_b64: str) -> tuple[str, int, int]:
    raw = base64.b64decode(data_b64)
    arr = np.frombuffer(raw, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)

    resized = cv2.resize(img, (TARGET_WIDTH, TARGET_HEIGHT), interpolation=cv2.INTER_LINEAR)

    ok, buf = cv2.imencode(".jpg", resized, [cv2.IMWRITE_JPEG_QUALITY, JPEG_QUALITY])
    if not ok:
        raise ValueError("Failed to encode preprocessed frame")
    return base64.b64encode(buf.tobytes()).decode("ascii"), TARGET_WIDTH, TARGET_HEIGHT


def main() -> None:
    producer = make_producer()
    consumer = make_consumer(GROUP_ID, [INPUT_TOPIC])
    processed = 0

    logger.info("Preprocessor ready — %s → %s", INPUT_TOPIC, OUTPUT_TOPIC)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            envelope = json.loads(msg.value())
            processed_b64, w, h = preprocess(envelope["data"])

            out = json.dumps({
                "frame_number": envelope["frame_number"],
                "timestamp": envelope["timestamp"],
                "width": w,
                "height": h,
                "data": processed_b64,
            })

            produce_with_backpressure(producer, OUTPUT_TOPIC, str(envelope["frame_number"]), out)
            processed += 1

            if processed % 100 == 0:
                logger.info("Preprocessed %d frames", processed)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        consumer.close()
        logger.info("Preprocessor stopped — processed %d frames", processed)


if __name__ == "__main__":
    main()
