"""Statistics: aggregates unique car and person counts from tracking topics.

Consumes tracking.cars and tracking.persons in a single consumer group and
prints a summary every PRINT_INTERVAL_FRAMES frames.
"""

import json
import logging
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.kafka_client import make_consumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CAR_TOPIC = os.environ.get("CAR_TOPIC", "tracking.cars")
PERSON_TOPIC = os.environ.get("PERSON_TOPIC", "tracking.persons")
GROUP_ID = os.environ.get("GROUP_ID", "statistics-group")
PRINT_INTERVAL_FRAMES = int(os.environ.get("PRINT_INTERVAL_FRAMES", "30"))


class Stats:
    def __init__(self):
        self._lock = threading.Lock()
        self.unique_cars: set[int] = set()
        self.unique_persons: set[int] = set()
        self.frames_cars = 0
        self.frames_persons = 0

    def update(self, msg: dict) -> None:
        obj_type = msg.get("object_type", "")
        track_ids = {t["track_id"] for t in msg.get("tracks", [])}
        with self._lock:
            if obj_type == "car":
                self.unique_cars.update(track_ids)
                self.frames_cars += 1
            elif obj_type == "person":
                self.unique_persons.update(track_ids)
                self.frames_persons += 1

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "unique_cars": len(self.unique_cars),
                "unique_persons": len(self.unique_persons),
                "frames_cars": self.frames_cars,
                "frames_persons": self.frames_persons,
            }


def print_loop(stats: Stats, interval_sec: float = 5.0) -> None:
    while True:
        time.sleep(interval_sec)
        s = stats.snapshot()
        print(
            f"\n{'='*44}\n"
            f"  Unique Cars:    {s['unique_cars']:>6}\n"
            f"  Unique People:  {s['unique_persons']:>6}\n"
            f"  Frames (cars):  {s['frames_cars']:>6}\n"
            f"  Frames (people):{s['frames_persons']:>6}\n"
            f"{'='*44}",
            flush=True,
        )


def main() -> None:
    stats = Stats()
    consumer = make_consumer(GROUP_ID, [CAR_TOPIC, PERSON_TOPIC])

    printer = threading.Thread(target=print_loop, args=(stats,), daemon=True)
    printer.start()

    logger.info("Statistics service ready — consuming %s and %s", CAR_TOPIC, PERSON_TOPIC)
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
            stats.update(envelope)
            processed += 1

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        s = stats.snapshot()
        print(
            f"\n{'='*44}\n"
            f"  FINAL STATISTICS\n"
            f"  Unique Cars:    {s['unique_cars']:>6}\n"
            f"  Unique People:  {s['unique_persons']:>6}\n"
            f"{'='*44}",
            flush=True,
        )
        logger.info("Statistics service stopped — processed %d messages", processed)


if __name__ == "__main__":
    main()
