import os
import time
import logging
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

TOPICS = [
    NewTopic("frames.raw",         num_partitions=3, replication_factor=1,
             config={"max.message.bytes": str(5 * 1024 * 1024)}),
    NewTopic("frames.preprocessed", num_partitions=3, replication_factor=1,
             config={"max.message.bytes": str(5 * 1024 * 1024)}),
    NewTopic("detections.cars",    num_partitions=3, replication_factor=1,
             config={"max.message.bytes": str(256 * 1024)}),
    NewTopic("detections.persons", num_partitions=3, replication_factor=1,
             config={"max.message.bytes": str(256 * 1024)}),
    NewTopic("tracking.combined",  num_partitions=3, replication_factor=1,
             config={"max.message.bytes": str(256 * 1024)}),
    NewTopic("control.upload",     num_partitions=1, replication_factor=1,
             config={"max.message.bytes": str(4 * 1024)}),
    NewTopic("control.session_end", num_partitions=1, replication_factor=1,
             config={"max.message.bytes": str(4 * 1024)}),
]


def wait_for_broker(admin: AdminClient, retries: int = 30, delay: float = 5.0) -> None:
    for attempt in range(1, retries + 1):
        try:
            admin.list_topics(timeout=5)
            logger.info("Broker reachable")
            return
        except KafkaException as e:
            logger.warning("Broker not ready (attempt %d/%d): %s", attempt, retries, e)
            time.sleep(delay)
    raise RuntimeError("Broker never became ready")


def main() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    wait_for_broker(admin)

    existing = set(admin.list_topics(timeout=10).topics.keys())
    to_create = [t for t in TOPICS if t.topic not in existing]

    if not to_create:
        logger.info("All topics already exist")
        return

    futures = admin.create_topics(to_create)
    for topic, future in futures.items():
        try:
            future.result()
            logger.info("Created topic: %s", topic)
        except KafkaException as e:
            if "already exists" in str(e):
                logger.info("Topic already exists: %s", topic)
            else:
                raise


if __name__ == "__main__":
    main()
