package com.example.kafka.consumer;

import com.example.kafka.config.KafkaTopicConfig;
import com.example.kafka.event.NotificationEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Notification Consumer — Batch Mode
 *
 * Demonstrates the batch consumer strategy.
 * Instead of processing one message at a time, the listener receives
 * a List of NotificationEvents and handles them all in one call.
 *
 * WHY BATCH: Sending 100 emails in one batch API call is far more efficient
 * than 100 individual calls. Reduces per-message overhead significantly.
 *
 * WHEN TO USE: Email/SMS bulk sending, Elasticsearch bulk indexing,
 * bulk database inserts — anywhere the downstream operation benefits from batching.
 */
@Slf4j
@Service
public class NotificationConsumer {

    // ── Strategy: Batch Consumer ─────────────────────────────────────────────
    @KafkaListener(
            topics = KafkaTopicConfig.NOTIFICATIONS_TOPIC,
            groupId = "notification-service-group",
            containerFactory = "notificationKafkaListenerContainerFactory"
    )
    public void consumeNotificationsBatch(
            @Payload List<NotificationEvent> events,
            @Header(KafkaHeaders.RECEIVED_PARTITION) List<Integer> partitions,
            @Header(KafkaHeaders.OFFSET) List<Long> offsets,
            Acknowledgment acknowledgment) {

        log.info("Received batch of {} notifications", events.size());

        try {
            // Group by notification type for efficient bulk dispatch
            var emailNotifications = events.stream()
                    .filter(e -> e.getType() == NotificationEvent.NotificationType.EMAIL)
                    .toList();

            var smsNotifications = events.stream()
                    .filter(e -> e.getType() == NotificationEvent.NotificationType.SMS)
                    .toList();

            var pushNotifications = events.stream()
                    .filter(e -> e.getType() == NotificationEvent.NotificationType.PUSH)
                    .toList();

            // Simulate bulk dispatch
            if (!emailNotifications.isEmpty()) {
                log.info("Sending {} emails in bulk", emailNotifications.size());
                // emailService.sendBulk(emailNotifications);
            }
            if (!smsNotifications.isEmpty()) {
                log.info("Sending {} SMS in bulk", smsNotifications.size());
                // smsService.sendBulk(smsNotifications);
            }
            if (!pushNotifications.isEmpty()) {
                log.info("Sending {} push notifications in bulk", pushNotifications.size());
                // pushService.sendBulk(pushNotifications);
            }

            // Acknowledge the entire batch after all are processed
            acknowledgment.acknowledge();
            log.info("Batch of {} notifications processed and acknowledged", events.size());

        } catch (Exception ex) {
            log.error("Batch processing failed for {} notifications: {}", events.size(), ex.getMessage());
            throw ex;
        }
    }

    // ── Dead Letter Topic Consumer for Notifications ─────────────────────────
    @KafkaListener(
            topics = KafkaTopicConfig.NOTIFICATIONS_DLT,
            groupId = "notification-dlt-group",
            containerFactory = "notificationKafkaListenerContainerFactory"
    )
    public void consumeNotificationsDlt(
            @Payload List<NotificationEvent> events,
            Acknowledgment acknowledgment) {

        log.error("[DLT] {} notifications failed permanently", events.size());
        events.forEach(e -> log.error("  Failed notification: id={}, userId={}, type={}",
                e.getNotificationId(), e.getUserId(), e.getType()));

        acknowledgment.acknowledge();
    }
}
