# Kafka basics

`docker-compose.yaml` взято з прикладу confluentic:
https://github.com/confluentinc/cp-all-in-one/blob/8.2.0-post/cp-all-in-one/docker-compose.yml


## 1. Запуск
```bash
docker compose up -d
```

Control center доступний за посиланням http://localhost:9021

## 2. Створення топіку
```bash
docker exec broker kafka-topics \
  --bootstrap-server broker:29092 \
  --create \
  --topic test-topic \
  --partitions 3 \
  --replication-factor 3
```

<details open>
    <summary>Результат</summary>
    
    ```bash
    Created topic test-topic.
    ```
</details>

## 3. Вивести список топіків
```bash
docker exec broker kafka-topics \
  --bootstrap-server broker:29092 \
  --list
```

<details open>
    <summary>Результат</summary>
    
    ```bash
    __consumer_offsets
    __internal_confluent_only_broker_info
    __transaction_state
    _confluent-alerts
    _confluent-command
    _confluent-controlcenter-2-5-0-1-AlertHistoryStore-changelog
    _confluent-controlcenter-2-5-0-1-AlertHistoryStore-repartition
    _confluent-ksql-default__command_topic
    _confluent-link-metadata
    _confluent-telemetry-metrics
    _confluent_balancer_api_state
    _schemas
    default_ksql_processing_log
    docker-connect-configs
    docker-connect-offsets
    docker-connect-status
    test-topic
    ```
</details>

## 4. Відправка 10 повідомлень
```bash
for i in $(seq 1 10); do echo "message $i"; done \
  | docker exec -i broker kafka-console-producer \
      --bootstrap-server broker:29092 \
      --topic test-topic
```

## 5. Отримання повідомлень через console consumer
```bash
docker exec -it broker kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic test-topic \
  --from-beginning
```

<details open>
    <summary>Результат</summary>
    
    ```bash
    message 1
    message 2
    message 3
    message 4
    message 5
    message 6
    message 7
    message 8
    message 9
    message 10
    ```
</details>

## 6. Видалення топіку
```bash
docker exec broker kafka-topics \
  --bootstrap-server broker:29092 \
  --delete \
  --topic test-topic
```

Перевіряємо, що топік видалено (очікуємо пустий результат):
```bash
docker exec broker kafka-topics \
  --bootstrap-server broker:29092 \
  --list | grep "test-topic"
```

### 7. Зупинка кластеру

```bash
docker compose down
```