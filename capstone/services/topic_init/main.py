"""One-shot service: creates all Kafka topics required by the pipeline."""

import os
import sys
import time
import logging

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")

TOPICS = [
    NewTopic("frames.raw",            num_partitions=3, replication_factor=1,
             config={"max.message.bytes": "5242880"}),
    NewTopic("frames.preprocessed",   num_partitions=3, replication_factor=1,
             config={"max.message.bytes": "5242880"}),
    NewTopic("detections.cars",       num_partitions=3, replication_factor=1,
             config={"max.message.bytes": "262144"}),
    NewTopic("detections.persons",    num_partitions=3, replication_factor=1,
             config={"max.message.bytes": "262144"}),
    NewTopic("tracking.cars",         num_partitions=1, replication_factor=1,
             config={"max.message.bytes": "262144"}),
    NewTopic("tracking.persons",      num_partitions=1, replication_factor=1,
             config={"max.message.bytes": "262144"}),
]

MAX_RETRIES = 30
RETRY_DELAY = 5


def wait_for_kafka(admin: AdminClient) -> None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            admin.list_topics(timeout=5)
            logger.info("Kafka is ready")
            return
        except KafkaException as exc:
            logger.warning("Kafka not ready (attempt %d/%d): %s", attempt, MAX_RETRIES, exc)
            time.sleep(RETRY_DELAY)
    logger.error("Kafka not available after %d retries — exiting", MAX_RETRIES)
    sys.exit(1)


def main() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    wait_for_kafka(admin)

    existing = set(admin.list_topics(timeout=10).topics.keys())
    to_create = [t for t in TOPICS if t.topic not in existing]

    if not to_create:
        logger.info("All topics already exist — nothing to do")
        return

    futures = admin.create_topics(to_create)
    for topic, future in futures.items():
        try:
            future.result()
            logger.info("Created topic: %s", topic)
        except KafkaException as exc:
            # TOPIC_ALREADY_EXISTS is not an error in our context
            if "TOPIC_ALREADY_EXISTS" in str(exc) or "already exists" in str(exc).lower():
                logger.info("Topic already exists: %s", topic)
            else:
                logger.error("Failed to create topic %s: %s", topic, exc)
                sys.exit(1)

    logger.info("Topic initialisation complete")


if __name__ == "__main__":
    main()
