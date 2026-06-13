import base64
import json
import logging
import os

import cv2
import numpy as np
from ultralytics import YOLO

from common.kafka_client import make_consumer, make_producer, produce_with_backpressure

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
# CLASS_IDS: comma-separated COCO class IDs to detect
#   car-detector:    CLASS_IDS=2,5,7  (car, bus, truck)
#   person-detector: CLASS_IDS=0      (person)
CLASS_IDS = [int(x) for x in os.environ["CLASS_IDS"].split(",")]
OUTPUT_TOPIC = os.environ["OUTPUT_TOPIC"]   # detections.cars or detections.persons
GROUP_ID = os.environ["GROUP_ID"]           # detector-cars or detector-persons
CONF_THRESHOLD = float(os.environ.get("CONF_THRESHOLD", "0.4"))
MODEL_PATH = os.environ.get("MODEL_PATH", "yolov8n.pt")

# "cpu" → CPU inference; "cuda" → NVIDIA GPU (image must have been built with PROCESSING_UNIT_TYPE=cuda)
_PROCESSING_UNIT_TYPE = os.environ.get("PROCESSING_UNIT_TYPE", "cpu")
_DEVICE = "cpu" if _PROCESSING_UNIT_TYPE == "cpu" else "cuda"

COCO_NAMES = {
    0: "person", 2: "car", 5: "bus", 7: "truck",
}


def run_inference(frame: np.ndarray) -> list[dict]:
    results = model.predict(frame, classes=CLASS_IDS, conf=CONF_THRESHOLD, device=_DEVICE, verbose=False)
    detections = []
    for r in results:
        for box in r.boxes:
            cls_id = int(box.cls[0])
            x1, y1, x2, y2 = (int(v) for v in box.xyxy[0].tolist())
            detections.append({
                "bbox": [x1, y1, x2, y2],
                "confidence": float(box.conf[0]),
                "class_id": cls_id,
                "class_name": COCO_NAMES.get(cls_id, str(cls_id)),
            })
    return detections


def process(msg_value: bytes, producer) -> None:
    payload = json.loads(msg_value)
    raw = base64.b64decode(payload["data"])
    arr = np.frombuffer(raw, dtype=np.uint8)
    frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if frame is None:
        return

    detections = run_inference(frame)

    out = json.dumps({
        "session_id": payload["session_id"],
        "frame_number": payload["frame_number"],
        "video_timestamp_ms": payload["video_timestamp_ms"],
        "detections": detections,
    }).encode()
    produce_with_backpressure(producer, OUTPUT_TOPIC, payload["session_id"], out)


def main() -> None:
    global model
    logger.info("Loading YOLOv8n model  class_ids=%s  device=%s", CLASS_IDS, _DEVICE)
    model = YOLO(MODEL_PATH)
    logger.info("Model loaded. Subscribing to frames.preprocessed …")

    consumer = make_consumer(BOOTSTRAP, ["frames.preprocessed"], GROUP_ID)
    producer = make_producer(BOOTSTRAP)

    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        try:
            process(msg.value(), producer)
        except Exception as e:
            logger.error("Detector error: %s", e)


if __name__ == "__main__":
    main()
