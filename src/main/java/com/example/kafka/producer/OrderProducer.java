package com.example.kafka.producer;

import com.example.kafka.config.KafkaTopicConfig;
import com.example.kafka.event.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Order Producer
 *
 * Demonstrates both async and sync producer strategies,
 * as well as keyed message routing for ordering guarantees.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducer {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    // ── Strategy 1: Async Send (Default) ────────────────────────────────────
    /**
     * Sends an order event asynchronously.
     *
     * KEY = userId ensures all events for the same user go to the same partition,
     * guaranteeing ordering for that user's order lifecycle.
     *
     * WHY ASYNC: Non-blocking. The producer doesn't stall the calling thread.
     * The callback handles success/failure without blocking.
     *
     * WHEN TO USE: Most event publishing in microservices. Use when the caller
     * doesn't need immediate confirmation before responding.
     */
    public void sendOrderAsync(OrderEvent event) {
        String key = event.getUserId();   // Route by userId → same partition → ordered

        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(KafkaTopicConfig.ORDERS_TOPIC, key, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[ASYNC] Failed to send order {}. Error: {}",
                        event.getOrderId(), ex.getMessage());
            } else {
                log.info("[ASYNC] Order {} sent → topic={}, partition={}, offset={}",
                        event.getOrderId(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }

    // ── Strategy 2: Sync Send ────────────────────────────────────────────────
    /**
     * Sends an order event synchronously and blocks until the broker confirms.
     *
     * WHY SYNC: Guarantees the message is persisted before the method returns.
     * Use when the caller needs to know the message was accepted before proceeding.
     *
     * WHEN TO USE: REST endpoints that need to confirm publish before responding
     * to the client. Avoid in high-throughput loops — blocking kills throughput.
     */
    public SendResult<String, OrderEvent> sendOrderSync(OrderEvent event)
            throws ExecutionException, InterruptedException {

        String key = event.getUserId();

        SendResult<String, OrderEvent> result =
                kafkaTemplate.send(KafkaTopicConfig.ORDERS_TOPIC, key, event).get(); // Blocks

        log.info("[SYNC] Order {} confirmed → partition={}, offset={}",
                event.getOrderId(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());

        return result;
    }

    // ── Strategy 3: Transactional Send ──────────────────────────────────────
    /**
     * Sends within a Kafka transaction — exactly-once semantics.
     *
     * WHY TRANSACTIONAL: The broker will only make this message visible to
     * consumers after the transaction commits. If the transaction is rolled back,
     * the message is never seen. Requires consumers to use isolation-level=read_committed.
     *
     * WHEN TO USE: Payment confirmation, inventory deduction — operations where
     * duplicate processing would cause real-world harm.
     */
    public void sendOrderTransactional(OrderEvent event) {
        kafkaTemplate.executeInTransaction(ops -> {
            ops.send(KafkaTopicConfig.ORDERS_TOPIC, event.getUserId(), event);
            log.info("[TX] Order {} sent in transaction", event.getOrderId());
            return true;
        });
    }
}
