package com.example.kafka;

import com.example.kafka.config.KafkaTopicConfig;
import com.example.kafka.event.OrderEvent;
import com.example.kafka.producer.OrderProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration Test — uses EmbeddedKafka (no real broker needed).
 *
 * EmbeddedKafka spins up an in-memory Kafka broker for the duration of the test.
 * This allows testing the full produce → consume flow without any infrastructure.
 *
 * Run with: mvn test
 */
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
        partitions = 3,
        topics = {
                KafkaTopicConfig.ORDERS_TOPIC,
                KafkaTopicConfig.ORDERS_DLT,
                KafkaTopicConfig.NOTIFICATIONS_TOPIC,
                KafkaTopicConfig.NOTIFICATIONS_DLT
        },
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:9092",
                "port=9092"
        }
)
class OrderKafkaIntegrationTest {

    @Autowired
    private OrderProducer orderProducer;

    /**
     * Test: Async producer sends a message without blocking.
     * Verifies the send completes without exception.
     */
    @Test
    void asyncProducer_shouldSendWithoutBlocking() {
        OrderEvent event = buildOrder("user-test-1");

        // Should not throw — async send returns immediately
        orderProducer.sendOrderAsync(event);

        // In a real test, use a CountDownLatch in a test consumer to verify receipt
        // See the pattern below in the manual consumer test
    }

    /**
     * Test: Sync producer blocks until broker confirms.
     * Verifies the returned SendResult contains partition and offset.
     */
    @Test
    void syncProducer_shouldReturnSendResult() throws Exception {
        OrderEvent event = buildOrder("user-test-2");

        var result = orderProducer.sendOrderSync(event);

        assertThat(result).isNotNull();
        assertThat(result.getRecordMetadata().topic()).isEqualTo(KafkaTopicConfig.ORDERS_TOPIC);
        assertThat(result.getRecordMetadata().partition()).isGreaterThanOrEqualTo(0);
        assertThat(result.getRecordMetadata().offset()).isGreaterThanOrEqualTo(0);
    }

    /**
     * Test: Keyed messages for same userId should route to the same partition.
     */
    @Test
    void keyedMessages_sameUser_shouldGoToSamePartition() throws Exception {
        String userId = "user-ordering-test";

        var result1 = orderProducer.sendOrderSync(buildOrder(userId));
        var result2 = orderProducer.sendOrderSync(buildOrder(userId));

        int partition1 = result1.getRecordMetadata().partition();
        int partition2 = result2.getRecordMetadata().partition();

        // Same key → same partition → guaranteed ordering for this user
        assertThat(partition1).isEqualTo(partition2);
    }

    // ── Helper ───────────────────────────────────────────────────────────────
    private OrderEvent buildOrder(String userId) {
        return OrderEvent.builder()
                .orderId(UUID.randomUUID().toString())
                .userId(userId)
                .status(OrderEvent.OrderStatus.CREATED)
                .items(List.of(
                        OrderEvent.OrderItem.builder()
                                .productId("prod-001")
                                .productName("Test Product")
                                .quantity(1)
                                .unitPrice(new BigDecimal("99.99"))
                                .build()
                ))
                .totalAmount(new BigDecimal("99.99"))
                .currency("USD")
                .createdAt(Instant.now())
                .build();
    }
}
