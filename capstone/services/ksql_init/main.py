import os
import time
import logging
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KSQLDB_URL = os.environ.get("KSQLDB_URL", "http://ksqldb-server:8088")

STATEMENTS = [
    # Register source streams on existing application topics
    """CREATE STREAM IF NOT EXISTS tracking_cars_raw (
  session_id VARCHAR,
  frame_number INT,
  video_timestamp_ms DOUBLE,
  object_type VARCHAR,
  tracks ARRAY<STRUCT<track_id INT, bbox ARRAY<INT>>>,
  in_frame INT,
  total_unique INT
) WITH (KAFKA_TOPIC='tracking.cars', VALUE_FORMAT='JSON', PARTITIONS=3)""",

    """CREATE STREAM IF NOT EXISTS tracking_persons_raw (
  session_id VARCHAR,
  frame_number INT,
  video_timestamp_ms DOUBLE,
  object_type VARCHAR,
  tracks ARRAY<STRUCT<track_id INT, bbox ARRAY<INT>>>,
  in_frame INT,
  total_unique INT
) WITH (KAFKA_TOPIC='tracking.persons', VALUE_FORMAT='JSON', PARTITIONS=3)""",

    # Rekey both streams by (session_id, frame_number) for exact per-frame joining
    """CREATE STREAM IF NOT EXISTS tracking_cars_rekeyed
  WITH (KAFKA_TOPIC='tracking.cars.rekeyed', VALUE_FORMAT='JSON', PARTITIONS=3) AS
SELECT
  session_id,
  frame_number,
  video_timestamp_ms,
  object_type,
  tracks,
  in_frame,
  total_unique,
  session_id + '_' + CAST(frame_number AS VARCHAR) AS frame_key
FROM tracking_cars_raw
PARTITION BY session_id + '_' + CAST(frame_number AS VARCHAR)
EMIT CHANGES""",

    """CREATE STREAM IF NOT EXISTS tracking_persons_rekeyed
  WITH (KAFKA_TOPIC='tracking.persons.rekeyed', VALUE_FORMAT='JSON', PARTITIONS=3) AS
SELECT
  session_id,
  frame_number,
  video_timestamp_ms,
  object_type,
  tracks,
  in_frame,
  total_unique,
  session_id + '_' + CAST(frame_number AS VARCHAR) AS frame_key
FROM tracking_persons_raw
PARTITION BY session_id + '_' + CAST(frame_number AS VARCHAR)
EMIT CHANGES""",

    # Aggregate tables — ksqlDB state stores for unique object counts per session.
    # LATEST_BY_OFFSET(total_unique) keeps the most-recent cumulative unique count emitted by
    # the tracker (which maintains a per-session seen-set and increments total_unique).
    # The statistics service queries these via ksqlDB pull query instead of consuming Kafka.
    """CREATE TABLE IF NOT EXISTS session_car_stats
  WITH (KAFKA_TOPIC='session.car.stats', VALUE_FORMAT='JSON', PARTITIONS=3) AS
SELECT
  session_id,
  LATEST_BY_OFFSET(total_unique) AS cars_total
FROM tracking_cars_raw
GROUP BY session_id
EMIT CHANGES""",

    """CREATE TABLE IF NOT EXISTS session_person_stats
  WITH (KAFKA_TOPIC='session.person.stats', VALUE_FORMAT='JSON', PARTITIONS=3) AS
SELECT
  session_id,
  LATEST_BY_OFFSET(total_unique) AS persons_total
FROM tracking_persons_raw
GROUP BY session_id
EMIT CHANGES""",

    # Stream-stream LEFT JOIN within 2-second window keyed by (session_id, frame_number)
    # LEFT JOIN: always emits when car tracking arrives; persons columns are null if behind by >2s
    """CREATE STREAM IF NOT EXISTS tracking_combined
  WITH (KAFKA_TOPIC='tracking.combined', VALUE_FORMAT='JSON', PARTITIONS=3) AS
SELECT
  c.session_id AS session_id,
  c.frame_number AS frame_number,
  c.video_timestamp_ms AS video_timestamp_ms,
  c.tracks AS car_tracks,
  c.in_frame AS cars_in_frame,
  c.total_unique AS cars_total,
  p.tracks AS person_tracks,
  p.in_frame AS persons_in_frame,
  p.total_unique AS persons_total
FROM tracking_cars_rekeyed c
LEFT JOIN tracking_persons_rekeyed p
  WITHIN 2 SECONDS GRACE PERIOD 500 MILLISECONDS
  ON c.frame_key = p.frame_key
PARTITION BY c.session_id
EMIT CHANGES""",
]


def wait_for_ksqldb(retries: int = 40, delay: float = 5.0) -> None:
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(f"{KSQLDB_URL}/info", timeout=5)
            if r.status_code == 200:
                logger.info("ksqlDB ready: %s", r.json().get("KsqlServerInfo", {}).get("version", "?"))
                return
        except requests.RequestException as e:
            logger.warning("ksqlDB not ready (attempt %d/%d): %s", attempt, retries, e)
        time.sleep(delay)
    raise RuntimeError(f"ksqlDB at {KSQLDB_URL} never became ready")


def run_statement(stmt: str) -> None:
    payload = {"ksql": stmt + ";", "streamsProperties": {}}
    r = requests.post(f"{KSQLDB_URL}/ksql", json=payload, timeout=30)
    if r.status_code in (200, 201):
        logger.info("OK: %.60s…", stmt.strip().replace("\n", " "))
    elif r.status_code == 400 and "already exists" in r.text:
        logger.info("Already exists (skip): %.60s…", stmt.strip().replace("\n", " "))
    else:
        raise RuntimeError(f"ksqlDB error {r.status_code}: {r.text[:300]}")


def main() -> None:
    wait_for_ksqldb()
    for stmt in STATEMENTS:
        run_statement(stmt)
    logger.info("ksqlDB streams created successfully")


if __name__ == "__main__":
    main()
