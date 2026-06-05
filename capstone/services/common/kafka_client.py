import time
import logging
from confluent_kafka import Producer, Consumer, KafkaException

logger = logging.getLogger(__name__)

_PRODUCER_CONF_BASE = {
    "message.max.bytes": 10 * 1024 * 1024,
    "queue.buffering.max.messages": 100_000,
    "queue.buffering.max.kbytes": 1_048_576,
    "queue.buffering.max.ms": 50,
}

_CONSUMER_CONF_BASE = {
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "fetch.message.max.bytes": 10 * 1024 * 1024,
    "max.partition.fetch.bytes": 10 * 1024 * 1024,
}


def make_producer(bootstrap_servers: str, retries: int = 30, delay: float = 5.0) -> Producer:
    conf = {"bootstrap.servers": bootstrap_servers, **_PRODUCER_CONF_BASE}
    for attempt in range(1, retries + 1):
        try:
            p = Producer(conf)
            # Probe connectivity
            p.list_topics(timeout=5)
            logger.info("Producer connected to %s", bootstrap_servers)
            return p
        except KafkaException as e:
            logger.warning("Producer attempt %d/%d failed: %s", attempt, retries, e)
            if attempt < retries:
                time.sleep(delay)
    raise RuntimeError(f"Could not connect producer to {bootstrap_servers} after {retries} attempts")


def make_consumer(
    bootstrap_servers: str,
    topics: list[str],
    group_id: str,
    retries: int = 30,
    delay: float = 5.0,
) -> Consumer:
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        **_CONSUMER_CONF_BASE,
    }
    for attempt in range(1, retries + 1):
        try:
            c = Consumer(conf)
            c.subscribe(topics)
            logger.info("Consumer group=%s subscribed to %s", group_id, topics)
            return c
        except KafkaException as e:
            logger.warning("Consumer attempt %d/%d failed: %s", attempt, retries, e)
            if attempt < retries:
                time.sleep(delay)
    raise RuntimeError(f"Could not connect consumer to {bootstrap_servers} after {retries} attempts")


def _lower(obj):
    """Recursively lowercase all dict keys (ksqlDB serialises field names in UPPERCASE)."""
    if isinstance(obj, dict):
        return {k.lower(): _lower(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_lower(i) for i in obj]
    return obj


def produce_with_backpressure(
    producer: Producer,
    topic: str,
    key: str,
    value: bytes,
    max_retries: int = 10,
) -> None:
    for _ in range(max_retries):
        try:
            producer.produce(topic, key=key, value=value)
            producer.poll(0)
            return
        except BufferError:
            logger.debug("Producer queue full on %s, draining…", topic)
            producer.poll(1.0)
    raise RuntimeError(f"Could not produce to {topic} after {max_retries} retries")
