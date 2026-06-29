# Kafka + Spring Boot Demo Application

A complete working example demonstrating all major Kafka strategies in a Spring Boot microservice.

## Project Structure

```
kafka-springboot-demo/
├── src/main/java/com/example/kafka/
│   ├── KafkaDemoApplication.java          # Entry point
│   ├── config/
│   │   ├── KafkaTopicConfig.java          # Topic definitions (partitions, replicas, compaction)
│   │   └── KafkaConsumerConfig.java       # Consumer factories, error handler, DLT setup
│   ├── event/
│   │   ├── OrderEvent.java                # Order domain event (Kafka message value)
│   │   └── NotificationEvent.java         # Notification domain event
│   ├── producer/
│   │   ├── OrderProducer.java             # Async, Sync, and Transactional strategies
│   │   └── NotificationProducer.java      # Async producer for notifications
│   ├── consumer/
│   │   ├── OrderConsumer.java             # Manual ack + DLT consumer
│   │   └── NotificationConsumer.java      # Batch consumer + DLT consumer
│   ├── service/
│   │   └── OrderProcessingService.java    # Business logic + Saga choreography
│   └── controller/
│       └── OrderController.java           # REST API to trigger Kafka flows
└── src/test/java/com/example/kafka/
    └── OrderKafkaIntegrationTest.java     # EmbeddedKafka integration tests
```

## Kafka Strategies Implemented

| Strategy | Where |
|---|---|
| At-least-once delivery | `KafkaConsumerConfig` — `acks=all`, manual ack |
| Keyed messages (ordering) | `OrderProducer` — key = userId |
| Async producer | `OrderProducer.sendOrderAsync()` |
| Sync producer | `OrderProducer.sendOrderSync()` |
| Transactional producer | `OrderProducer.sendOrderTransactional()` |
| Consumer group + concurrency | `KafkaConsumerConfig` — concurrency=3 |
| Manual acknowledgment | `OrderConsumer`, `NotificationConsumer` |
| Batch consumer | `NotificationConsumer` |
| Exponential backoff retry | `KafkaConsumerConfig.errorHandler()` |
| Dead Letter Topic | `KafkaConsumerConfig` + `OrderConsumer.consumeOrderDlt()` |
| Non-retryable exceptions | `KafkaConsumerConfig.errorHandler()` |
| Saga choreography | `OrderProcessingService` — orders → notifications |
| Compacted topic | `KafkaTopicConfig.userProfilesTopic()` |

## Prerequisites

- Java 17+
- Maven 3.8+
- Docker (for Kafka)

## Running Kafka Locally

```bash
# Start Kafka with Docker Compose (single-node)
docker run -d \
  --name kafka \
  -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  apache/kafka:3.7.0
```

## Running the Application

```bash
mvn spring-boot:run
```

## Testing the Kafka Flows

### 1. Async Publish (fire-and-forget)
```bash
curl -X POST "http://localhost:8080/api/orders/async?userId=user-123"
```

### 2. Sync Publish (blocking, returns partition + offset)
```bash
curl -X POST "http://localhost:8080/api/orders/sync?userId=user-456"
```

### 3. Transactional Publish (exactly-once)
```bash
curl -X POST "http://localhost:8080/api/orders/tx?userId=user-789"
```

### 4. Trigger DLT Flow (order will fail → retry 3x → dead letter)
```bash
curl -X POST "http://localhost:8080/api/orders/fail?userId=user-999"
# Watch the logs — you'll see 3 retry attempts then a DLT consumer log
```

### 5. Test keyed ordering (same userId → same partition)
```bash
# Send multiple orders for the same user — all go to the same partition
curl -X POST "http://localhost:8080/api/orders/sync?userId=user-123"
curl -X POST "http://localhost:8080/api/orders/sync?userId=user-123"
curl -X POST "http://localhost:8080/api/orders/sync?userId=user-123"
# All will show the same partition number in the response
```

## Running Tests

```bash
# Integration tests use EmbeddedKafka — no real broker needed
mvn test
```

## Event Flow (Saga)

```
POST /api/orders/async
        ↓
  OrderProducer
        ↓ (key = userId)
  "orders" topic (partition by userId)
        ↓
  OrderConsumer  ←── exponential backoff retry ←── error
        ↓ (success)
  OrderProcessingService
        ↓
  NotificationProducer
        ↓
  "notifications" topic
        ↓
  NotificationConsumer (batch mode)
        ↓
  Bulk email/SMS/push dispatch

  On failure after retries:
  "orders" topic → orders.DLT → OrderConsumer.consumeOrderDlt()
```
