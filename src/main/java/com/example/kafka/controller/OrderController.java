package com.example.kafka.controller;

import com.example.kafka.event.OrderEvent;
import com.example.kafka.producer.OrderProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * REST Controller — Order API
 *
 * Exposes endpoints to place orders using different Kafka producer strategies.
 * Use this to test the application end-to-end.
 *
 * Example requests:
 *   POST /api/orders/async      → fire-and-forget async publish
 *   POST /api/orders/sync       → blocking publish with confirmation
 *   POST /api/orders/tx         → transactional publish (exactly-once)
 *   POST /api/orders/fail       → triggers DLT flow (order ID starts with "FAIL")
 */
@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderProducer orderProducer;

    // ── POST /api/orders/async ───────────────────────────────────────────────
    @PostMapping("/async")
    public ResponseEntity<Map<String, String>> placeOrderAsync(
            @RequestParam(defaultValue = "user-123") String userId) {

        OrderEvent event = buildSampleOrder(userId);
        orderProducer.sendOrderAsync(event);

        return ResponseEntity.accepted().body(Map.of(
                "strategy", "async",
                "orderId", event.getOrderId(),
                "userId", userId,
                "message", "Order accepted and publishing asynchronously"
        ));
    }

    // ── POST /api/orders/sync ────────────────────────────────────────────────
    @PostMapping("/sync")
    public ResponseEntity<Map<String, Object>> placeOrderSync(
            @RequestParam(defaultValue = "user-456") String userId) throws ExecutionException, InterruptedException {

        OrderEvent event = buildSampleOrder(userId);
        var result = orderProducer.sendOrderSync(event);

        return ResponseEntity.ok(Map.of(
                "strategy", "sync",
                "orderId", event.getOrderId(),
                "userId", userId,
                "partition", result.getRecordMetadata().partition(),
                "offset", result.getRecordMetadata().offset(),
                "message", "Order confirmed by broker"
        ));
    }

    // ── POST /api/orders/tx ──────────────────────────────────────────────────
    @PostMapping("/tx")
    public ResponseEntity<Map<String, String>> placeOrderTransactional(
            @RequestParam(defaultValue = "user-789") String userId) {

        OrderEvent event = buildSampleOrder(userId);
        orderProducer.sendOrderTransactional(event);

        return ResponseEntity.ok(Map.of(
                "strategy", "transactional",
                "orderId", event.getOrderId(),
                "userId", userId,
                "message", "Order committed in Kafka transaction (exactly-once)"
        ));
    }

    // ── POST /api/orders/fail (triggers DLT) ────────────────────────────────
    @PostMapping("/fail")
    public ResponseEntity<Map<String, String>> placeFailingOrder(
            @RequestParam(defaultValue = "user-999") String userId) {

        // Order IDs starting with "FAIL" trigger simulated failures in OrderProcessingService
        // After 3 retry attempts, the message will land in orders.DLT
        OrderEvent event = buildSampleOrder(userId);
        event.setOrderId("FAIL-" + event.getOrderId());

        orderProducer.sendOrderAsync(event);

        return ResponseEntity.accepted().body(Map.of(
                "strategy", "async (will fail → DLT)",
                "orderId", event.getOrderId(),
                "userId", userId,
                "message", "Order published — will fail and end up in orders.DLT after retries"
        ));
    }

    // ── Helper ───────────────────────────────────────────────────────────────
    private OrderEvent buildSampleOrder(String userId) {
        return OrderEvent.builder()
                .orderId(UUID.randomUUID().toString())
                .userId(userId)
                .status(OrderEvent.OrderStatus.CREATED)
                .items(List.of(
                        OrderEvent.OrderItem.builder()
                                .productId("prod-001")
                                .productName("Wireless Headphones")
                                .quantity(1)
                                .unitPrice(new BigDecimal("149.99"))
                                .build(),
                        OrderEvent.OrderItem.builder()
                                .productId("prod-002")
                                .productName("Phone Case")
                                .quantity(2)
                                .unitPrice(new BigDecimal("19.99"))
                                .build()
                ))
                .totalAmount(new BigDecimal("189.97"))
                .currency("USD")
                .createdAt(Instant.now())
                .build();
    }
}
