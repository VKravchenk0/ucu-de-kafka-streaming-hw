"""Kafka producer/consumer factory with retry-on-startup logic."""

import os
import time
import logging

from confluent_kafka import Producer, Consumer, KafkaException

logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
_MAX_RETRIES = 30
_RETRY_DELAY = 5


def _wait_for_kafka(bootstrap_servers: str) -> None:
    from confluent_kafka.admin import AdminClient

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            admin = AdminClient({"bootstrap.servers": bootstrap_servers})
            meta = admin.list_topics(timeout=5)
            if meta:
                logger.info("Kafka ready at %s", bootstrap_servers)
                return
        except KafkaException as exc:
            logger.warning("Kafka not ready (attempt %d/%d): %s", attempt, _MAX_RETRIES, exc)
        time.sleep(_RETRY_DELAY)
    raise RuntimeError(f"Kafka not available after {_MAX_RETRIES} retries")


def make_producer(extra_config: dict | None = None) -> Producer:
    _wait_for_kafka(BOOTSTRAP_SERVERS)
    config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "message.max.bytes": 10_000_000,
        "queue.buffering.max.messages": 100_000,
        "queue.buffering.max.kbytes": 1_048_576,
        "queue.buffering.max.ms": 100,
    }
    if extra_config:
        config.update(extra_config)
    return Producer(config)


def make_consumer(group_id: str, topics: list[str], extra_config: dict | None = None) -> Consumer:
    _wait_for_kafka(BOOTSTRAP_SERVERS)
    config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "fetch.message.max.bytes": 10_000_000,
        "max.partition.fetch.bytes": 10_000_000,
    }
    if extra_config:
        config.update(extra_config)
    consumer = Consumer(config)
    consumer.subscribe(topics)
    return consumer


def delivery_report(err, msg):
    if err:
        logger.error("Delivery failed: %s", err)


def produce_with_backpressure(producer, topic: str, key: str, value: str, callback=None) -> None:
    """Produce a message, draining the queue if it's full."""
    while True:
        try:
            producer.produce(topic, key=key, value=value, callback=callback or delivery_report)
            producer.poll(0)
            return
        except BufferError:
            producer.poll(0.5)
