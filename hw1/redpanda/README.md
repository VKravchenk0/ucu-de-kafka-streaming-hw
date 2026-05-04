# Redpanda basics

Redpanda розгорнута на основі офіційного quickstart-туторіалу - https://docs.redpanda.com/current/get-started/quick-start/


## 1. Запуск
```bash
docker compose up -d
```

Адмін-консоль доступна за посиланням http://localhost:8080/login (superuser/secretpassword)

## 2. Створення топіку
```bash
docker exec redpanda-0 rpk topic create test-topic \
  --partitions 3 --replicas 3 \
  -X brokers=redpanda-0:9092 \
  -X user=superuser -X pass=secretpassword
```

<details open>
    <summary>Результат</summary>
    
    ```bash
    TOPIC       STATUS
    test-topic  OK
    ```
</details>

## 3. Вивести список топіків
```bash
docker exec redpanda-0 rpk topic list \
  -X brokers=redpanda-0:9092 \
  -X user=superuser -X pass=secretpassword
```

<details open>
    <summary>Результат</summary>
    
    ```bash
    NAME                      PARTITIONS  REPLICAS
    _redpanda.audit_log       12          3
    _redpanda.transform_logs  1           3
    _schemas                  1           3
    edu-filtered-domains      1           1
    logins                    1           1
    test-topic                3           3
    transactions              1           1
    ```
</details>

## 4. Відправка 10 повідомлень
```bash
for i in $(seq 1 10); do echo "message $i"; done \
  | docker exec -i redpanda-0 rpk topic produce test-topic \
      -X brokers=redpanda-0:9092 \
      -X user=superuser -X pass=secretpassword
```

<details open>
    <summary>Результат</summary>
    
    ```bash
    Produced to partition 0 at offset 0 with timestamp 1777669436424.
    Produced to partition 0 at offset 1 with timestamp 1777669436424.
    Produced to partition 0 at offset 2 with timestamp 1777669436424.
    Produced to partition 0 at offset 3 with timestamp 1777669436424.
    Produced to partition 0 at offset 4 with timestamp 1777669436424.
    Produced to partition 0 at offset 5 with timestamp 1777669436424.
    Produced to partition 0 at offset 6 with timestamp 1777669436424.
    Produced to partition 0 at offset 7 with timestamp 1777669436424.
    Produced to partition 0 at offset 8 with timestamp 1777669436424.
    Produced to partition 0 at offset 9 with timestamp 1777669436424.
    ```
</details>

## 5. Отримання повідомлень через console consumer
```bash
docker exec -it redpanda-0 rpk topic consume test-topic \
  --offset start \
  -X brokers=redpanda-0:9092 \
  -X user=superuser -X pass=secretpassword
```

<details open>
    <summary>Результат</summary>
    
    ```bash
    {
        "topic": "test-topic",
        "value": "message 1",
        "timestamp": 1777669436424,
        "partition": 0,
        "offset": 0
    }
    {
        "topic": "test-topic",
        "value": "message 2",
        "timestamp": 1777669436424,
        "partition": 0,
        "offset": 1
    }
    {
        "topic": "test-topic",
        "value": "message 3",
        "timestamp": 1777669436424,
        "partition": 0,
        "offset": 2
    }
    {
        "topic": "test-topic",
        "value": "message 4",
        "timestamp": 1777669436424,
        "partition": 0,
        "offset": 3
    }
    {
        "topic": "test-topic",
        "value": "message 5",
        "timestamp": 1777669436424,
        "partition": 0,
        "offset": 4
    }
    {
        "topic": "test-topic",
        "value": "message 6",
        "timestamp": 1777669436424,
        "partition": 0,
        "offset": 5
    }
    {
        "topic": "test-topic",
        "value": "message 7",
        "timestamp": 1777669436424,
        "partition": 0,
        "offset": 6
    }
    {
        "topic": "test-topic",
        "value": "message 8",
        "timestamp": 1777669436424,
        "partition": 0,
        "offset": 7
    }
    {
        "topic": "test-topic",
        "value": "message 9",
        "timestamp": 1777669436424,
        "partition": 0,
        "offset": 8
    }
    {
        "topic": "test-topic",
        "value": "message 10",
        "timestamp": 1777669436424,
        "partition": 0,
        "offset": 9
    }
    ```
</details>

## 6. Видалення топіку
```bash
docker exec redpanda-0 rpk topic delete test-topic \
  -X brokers=redpanda-0:9092 \
  -X user=superuser -X pass=secretpassword
```
<details open>
    <summary>Результат</summary>
    
    ```bash
    TOPIC       STATUS
    test-topic  OK
    ```
</details>

Перевіряємо, що топік видалено (очікуємо пустий результат):
```bash
docker exec redpanda-0 rpk topic list \
  -X brokers=redpanda-0:9092 \
  -X user=superuser -X pass=secretpassword | grep "test-topic"
```

### 7. Зупинка кластеру

```bash
docker compose down
```