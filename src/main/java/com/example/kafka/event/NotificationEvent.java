package com.example.kafka.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Event published when a notification needs to be sent.
 * Demonstrates a second topic / event type in the system.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationEvent {

    private String notificationId;
    private String userId;
    private NotificationType type;
    private String subject;
    private String message;
    private String referenceId;   // orderId, paymentId, etc.
    private Instant createdAt;

    public enum NotificationType {
        EMAIL, SMS, PUSH
    }
}
