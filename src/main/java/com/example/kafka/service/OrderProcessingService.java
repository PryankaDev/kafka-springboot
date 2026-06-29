package com.example.kafka.service;

import com.example.kafka.event.NotificationEvent;
import com.example.kafka.event.OrderEvent;
import com.example.kafka.producer.NotificationProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

/**
 * Order Processing Service
 *
 * Handles the business logic after an order event is consumed.
 * Also demonstrates the Saga / Choreography pattern:
 *   - Consumes: "orders" topic
 *   - Produces:  "notifications" topic (next step in the saga)
 *
 * This is the heart of the event-driven flow:
 *   OrderController → orders topic → OrderConsumer → OrderProcessingService
 *                                                        → notifications topic
 *                                                            → NotificationConsumer
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProcessingService {

    private final NotificationProducer notificationProducer;

    /**
     * Processes a successfully consumed order.
     * After processing, publishes a notification event — Saga choreography.
     */
    public void processOrder(OrderEvent event) {
        log.info("Processing order {} for user {} — status={}",
                event.getOrderId(), event.getUserId(), event.getStatus());

        // ── Simulate business logic ──────────────────────────────────────────
        // In a real service:
        //   - validate inventory
        //   - persist to DB
        //   - call payment service
        //   - update order status
        simulateProcessing(event);

        // ── Saga: Publish next event in the chain ────────────────────────────
        // This service (InventoryService/OrderService) publishes a notification event
        // The NotificationService will consume it independently
        NotificationEvent notification = NotificationEvent.builder()
                .notificationId(UUID.randomUUID().toString())
                .userId(event.getUserId())
                .type(NotificationEvent.NotificationType.EMAIL)
                .subject("Order Confirmed: " + event.getOrderId())
                .message("Your order has been confirmed. Total: "
                        + event.getTotalAmount() + " " + event.getCurrency())
                .referenceId(event.getOrderId())
                .createdAt(Instant.now())
                .build();

        notificationProducer.sendNotification(notification);
        log.info("Saga: notification event published for order {}", event.getOrderId());
    }

    /**
     * Handles an order that failed all retry attempts (received from DLT).
     * In production: persist to dead_orders table, alert ops, create support ticket.
     */
    public void handleFailedOrder(OrderEvent event) {
        log.error("Handling permanently failed order {}. Manual intervention required.", event.getOrderId());

        // In production:
        // failedOrderRepository.save(FailedOrder.from(event));
        // alertingService.createIncident("Order processing failed", event);
        // slackService.notify("#ops-alerts", "Dead letter: order " + event.getOrderId());
    }

    private void simulateProcessing(OrderEvent event) {
        // Simulate occasional transient failures for demo purposes
        // In a real app, this would call downstream services
        if (event.getOrderId() != null && event.getOrderId().startsWith("FAIL")) {
            throw new RuntimeException("Simulated transient failure for order " + event.getOrderId());
        }
        log.debug("Order {} business logic completed successfully", event.getOrderId());
    }
}
