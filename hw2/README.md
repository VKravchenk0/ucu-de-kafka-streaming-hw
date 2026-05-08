Three microservices in hw2/:

`hw2/producer/` — reads input.mp4 as 64KB chunks, spawns `NUM_PRODUCERS` threads, sends each chunk as a Kafka message with a `send_time_ms` header
`hw2/consumer/` — spawns `NUM_CONSUMERS` threads (platform threads, each owning its own `KafkaConsumer`), simulates 1s processing, writes timestamps to `output/consumer-N.csv`
`hw2/stat-aggregator/` — reads all *.csv files and reports throughput (Mbps) and max latency
Run commands:

Config 1 (Kafka in Docker, apps standalone):
```bash
# docker compose --profile infra up -d broker broker-2 broker-3
docker compose --profile infra up -d
java -jar producer/target/producer-1.0-SNAPSHOT.jar --bootstrap-servers localhost:9092 --num-producers 1 --num-partitions 1
java -jar consumer/target/consumer-1.0-SNAPSHOT.jar --bootstrap-servers localhost:9092 --num-consumers 1
# Ctrl+C consumer when done, then:
java -jar stat-aggregator/target/stat-aggregator-1.0-SNAPSHOT.jar --output-dir ./output
```

Config 2 (fully Docker):
```bash
docker compose --profile infra up -d
# docker compose --profile infra up -d broker broker-2 broker-3
docker compose up --build consumer
docker compose run --build --rm producer
# wait for consumer to finish, then:
docker compose stop consumer
docker compose run --build --rm stat-aggregator

#docker compose build --no-cache producer consumer stat-aggregator
```


All parameters (`NUM_PRODUCERS`, `NUM_CONSUMERS`, `NUM_PARTITIONS`, `TOPIC_NAME`, etc.) are configurable via `.env` for Docker or CLI flags for standalone runs.