package com.example.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Kafka + Spring Boot Demo Application
 *
 * Demonstrates:
 *  - At-least-once delivery with manual acknowledgment
 *  - Keyed message routing for ordering guarantees
 *  - Async and sync producer strategies
 *  - Exponential backoff retry
 *  - Dead Letter Topic (DLT) for unrecoverable messages
 *  - Batch consumer
 *  - Consumer groups with concurrency
 *  - Topic auto-creation with partitions and replicas
 */
@SpringBootApplication
public class KafkaDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaDemoApplication.class, args);
    }
}
