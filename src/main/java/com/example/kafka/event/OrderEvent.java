package com.example.kafka.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Domain event published when an order is placed.
 *
 * Used as the Kafka message value. Serialized to JSON by
 * Spring's JsonSerializer and deserialized by JsonDeserializer.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent {

    private String orderId;
    private String userId;         // Used as Kafka message KEY → partition routing
    private OrderStatus status;
    private List<OrderItem> items;
    private BigDecimal totalAmount;
    private String currency;
    private Instant createdAt;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderItem {
        private String productId;
        private String productName;
        private int quantity;
        private BigDecimal unitPrice;
    }

    public enum OrderStatus {
        CREATED, CONFIRMED, PROCESSING, SHIPPED, DELIVERED, CANCELLED
    }
}
