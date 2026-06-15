import base64
import json
import logging
import os
import threading

import cv2

from common.kafka_client import make_consumer, make_producer, produce_with_backpressure

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
FRAME_INTERVAL = int(os.environ.get("FRAME_INTERVAL", "1"))   # emit every Nth frame
JPEG_QUALITY = int(os.environ.get("JPEG_QUALITY", "85"))


def process_video(session_id: str, file_path: str) -> None:
    producer = make_producer(BOOTSTRAP)
    cap = cv2.VideoCapture(file_path)
    if not cap.isOpened():
        logger.error("Cannot open %s for session %s", file_path, session_id)
        return

    frame_number = 0
    emitted = 0
    logger.info("session=%s  starting  file=%s", session_id, file_path)

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        video_timestamp_ms = cap.get(cv2.CAP_PROP_POS_MSEC)

        if frame_number % FRAME_INTERVAL == 0:
            ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, JPEG_QUALITY])
            if ok:
                h, w = frame.shape[:2]
                msg = json.dumps({
                    "session_id": session_id,
                    "frame_number": emitted,
                    "video_timestamp_ms": video_timestamp_ms,
                    "width": w,
                    "height": h,
                    "data": base64.b64encode(buf.tobytes()).decode(),
                }).encode()
                produce_with_backpressure(producer, "frames.raw", session_id, msg)
                emitted += 1

        frame_number += 1

    cap.release()
    producer.flush()

    end_msg = json.dumps({"session_id": session_id, "total_frames": emitted}).encode()
    produce_with_backpressure(producer, "control.session_end", session_id, end_msg)
    producer.flush()
    logger.info("session=%s  done  frames_emitted=%d", session_id, emitted)


def main() -> None:
    consumer = make_consumer(BOOTSTRAP, ["control.upload"], "generator-control-group")
    logger.info("Waiting for upload events…")

    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        try:
            payload = json.loads(msg.value())
            session_id = payload["session_id"]
            file_path = payload["file_path"]
            t = threading.Thread(target=process_video, args=(session_id, file_path), daemon=True)
            t.start()
        except Exception as e:
            logger.error("Bad control.upload message: %s", e)


if __name__ == "__main__":
    main()
