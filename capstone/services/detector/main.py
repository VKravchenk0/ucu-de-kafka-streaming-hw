"""Detector: runs YOLOv8n on preprocessed frames and publishes detections.

Env vars:
  CLASS_IDS   comma-separated COCO class IDs to keep (e.g. "2,5,7" for cars)
  INPUT_TOPIC  (default: frames.preprocessed)
  OUTPUT_TOPIC (default: detections.cars)
  GROUP_ID     consumer group id
  CONFIDENCE   minimum detection confidence (default: 0.4)
"""

import base64
import json
import logging
import os
import sys

import cv2
import numpy as np
from ultralytics import YOLO

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.kafka_client import make_producer, make_consumer, produce_with_backpressure

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CLASS_IDS = set(int(x) for x in os.environ.get("CLASS_IDS", "2,5,7").split(","))
INPUT_TOPIC = os.environ.get("INPUT_TOPIC", "frames.preprocessed")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "detections.cars")
GROUP_ID = os.environ.get("GROUP_ID", f"detector-{OUTPUT_TOPIC}")
CONFIDENCE = float(os.environ.get("CONFIDENCE", "0.4"))
MODEL_PATH = os.environ.get("MODEL_PATH", "yolov8n.pt")

COCO_NAMES = {
    0: "person", 1: "bicycle", 2: "car", 3: "motorcycle",
    5: "bus", 7: "truck", 9: "traffic light",
}


def decode_frame(data_b64: str) -> np.ndarray:
    raw = base64.b64decode(data_b64)
    arr = np.frombuffer(raw, dtype=np.uint8)
    return cv2.imdecode(arr, cv2.IMREAD_COLOR)


def detect(model: YOLO, img: np.ndarray) -> list[dict]:
    results = model.predict(img, conf=CONFIDENCE, verbose=False)[0]
    detections = []
    for box in results.boxes:
        cls_id = int(box.cls[0])
        if cls_id not in CLASS_IDS:
            continue
        x1, y1, x2, y2 = box.xyxy[0].tolist()
        detections.append({
            "bbox": [x1, y1, x2, y2],
            "confidence": float(box.conf[0]),
            "class_id": cls_id,
            "class_name": COCO_NAMES.get(cls_id, str(cls_id)),
        })
    return detections


def main() -> None:
    logger.info("Loading YOLO model: %s", MODEL_PATH)
    model = YOLO(MODEL_PATH)
    logger.info("Detector ready — classes=%s  %s → %s", CLASS_IDS, INPUT_TOPIC, OUTPUT_TOPIC)

    producer = make_producer()
    consumer = make_consumer(GROUP_ID, [INPUT_TOPIC])
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
            img = decode_frame(envelope["data"])
            dets = detect(model, img)

            out = json.dumps({
                "frame_number": envelope["frame_number"],
                "timestamp": envelope["timestamp"],
                "detections": dets,
            })

            produce_with_backpressure(producer, OUTPUT_TOPIC, str(envelope["frame_number"]), out)
            processed += 1

            if processed % 50 == 0:
                logger.info("Processed %d frames, last had %d detections", processed, len(dets))

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        consumer.close()
        logger.info("Detector stopped — processed %d frames", processed)


if __name__ == "__main__":
    main()
