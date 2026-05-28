"""Generator: splits input.mp4 into JPEG frames and publishes them to frames.raw."""

import base64
import json
import logging
import os
import sys
import time

import cv2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.kafka_client import make_producer, produce_with_backpressure

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

VIDEO_PATH = os.environ.get("VIDEO_PATH", "/data/input.mp4")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "frames.raw")
FRAME_INTERVAL = int(os.environ.get("FRAME_INTERVAL", "1"))     # send every Nth frame
TARGET_WIDTH = int(os.environ.get("TARGET_WIDTH", "640"))
TARGET_HEIGHT = int(os.environ.get("TARGET_HEIGHT", "480"))
JPEG_QUALITY = int(os.environ.get("JPEG_QUALITY", "85"))
LOOP = os.environ.get("LOOP", "false").lower() == "true"        # replay video in a loop


def process_video(producer) -> None:
    cap = cv2.VideoCapture(VIDEO_PATH)
    if not cap.isOpened():
        logger.error("Cannot open video: %s", VIDEO_PATH)
        sys.exit(1)

    fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
    total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    logger.info("Video: %s  fps=%.1f  frames=%d", VIDEO_PATH, fps, total)

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
            "frame_number": frame_number,
            "timestamp": time.time(),
            "width": TARGET_WIDTH,
            "height": TARGET_HEIGHT,
            "data": base64.b64encode(buf.tobytes()).decode("ascii"),
        })

        produce_with_backpressure(producer, OUTPUT_TOPIC, str(frame_number), payload)
        sent += 1

        if sent % 100 == 0:
            logger.info("Sent %d frames (frame_number=%d)", sent, frame_number)

    cap.release()
    producer.flush()
    logger.info("Generator done — sent %d frames total", sent)


def main() -> None:
    producer = make_producer()
    while True:
        process_video(producer)
        if not LOOP:
            break
        logger.info("Video finished — replaying (LOOP=true)")


if __name__ == "__main__":
    main()
