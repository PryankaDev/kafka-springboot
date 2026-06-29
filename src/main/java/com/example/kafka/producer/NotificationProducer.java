package com.example.kafka.producer;

import com.example.kafka.config.KafkaTopicConfig;
import com.example.kafka.event.NotificationEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Notification Producer
 *
 * Publishes notification events to the notifications topic.
 * Notifications are keyed by userId so all notifications for a user
 * go to the same partition (preserving delivery order per user).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationProducer {

    private final KafkaTemplate<String, NotificationEvent> kafkaTemplate;

    public void sendNotification(NotificationEvent event) {
        kafkaTemplate.send(KafkaTopicConfig.NOTIFICATIONS_TOPIC, event.getUserId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send notification {}: {}", event.getNotificationId(), ex.getMessage());
                    } else {
                        log.info("Notification {} queued → partition={}",
                                event.getNotificationId(),
                                result.getRecordMetadata().partition());
                    }
                });
    }
}
