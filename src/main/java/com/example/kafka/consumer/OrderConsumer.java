package com.example.kafka.consumer;

import com.example.kafka.config.KafkaTopicConfig;
import com.example.kafka.event.OrderEvent;
import com.example.kafka.service.OrderProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

/**
 * Order Consumer
 *
 * Demonstrates:
 *  - Manual acknowledgment (at-least-once delivery)
 *  - Consumer group with concurrency (defined in KafkaConsumerConfig)
 *  - Header inspection for observability
 *  - Dead Letter Topic handling
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderConsumer {

    private final OrderProcessingService orderProcessingService;

    // ── Strategy: Manual Acknowledgment + At-Least-Once ─────────────────────
    /**
     * Main order consumer.
     *
     * containerFactory = "orderKafkaListenerContainerFactory" links this listener
     * to our custom factory (3 concurrent threads, exponential backoff, DLT).
     *
     * Acknowledgment is injected and committed ONLY after successful processing.
     * If an exception is thrown, Spring Kafka's error handler retries the message.
     *
     * Partition and offset headers are injected for logging/tracing.
     */
    @KafkaListener(
            topics = KafkaTopicConfig.ORDERS_TOPIC,
            groupId = "order-service-group",
            containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void consumeOrder(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key) {

        log.info("Received order {} from partition={}, offset={}, key={}",
                event.getOrderId(), partition, offset, key);

        try {
            // Process the order — this may throw if downstream services are unavailable
            orderProcessingService.processOrder(event);
            // ✅ No acknowledgment.acknowledge() — RECORD mode + tx commit handles offset
            log.info("Order {} processed and acknowledged", event.getOrderId());

        } catch (Exception ex) {
            // Do NOT acknowledge — error handler will retry or send to DLT
            log.error("Failed to process order {}. Will retry. Error: {}",
                    event.getOrderId(), ex.getMessage());
            throw ex;  // Re-throw so Spring Kafka's error handler takes over
        }
    }

    // ── Dead Letter Topic Consumer ───────────────────────────────────────────
    /**
     * Consumes messages that failed all retries on the main orders topic.
     *
     * In production: alert ops team, store for manual review, trigger compensating action.
     */
    @KafkaListener(
            topics = KafkaTopicConfig.ORDERS_DLT,
            groupId = "order-dlt-group",
            containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void consumeOrderDlt(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        log.error("[DLT] Dead letter received for order {} — partition={}, offset={}",
                event.getOrderId(), partition, offset);

        // In production: save to a "failed_orders" table, alert ops, create support ticket
        orderProcessingService.handleFailedOrder(event);

        acknowledgment.acknowledge();
    }
}
