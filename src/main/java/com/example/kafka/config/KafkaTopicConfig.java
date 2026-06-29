package com.example.kafka.config;

import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.apache.kafka.clients.admin.NewTopic;

/**
 * Topic Configuration
 *
 * Defines all Kafka topics used in the application.
 * Spring will auto-create these topics on startup if they don't exist
 * (requires Kafka broker with auto.create.topics.enable=true or admin privileges).
 */
@Configuration
public class KafkaTopicConfig {

    // ── Topic Name Constants ─────────────────────────────────────────────────
    public static final String ORDERS_TOPIC          = "orders";
    public static final String ORDERS_DLT            = "orders.DLT";
    public static final String NOTIFICATIONS_TOPIC   = "notifications";
    public static final String NOTIFICATIONS_DLT     = "notifications.DLT";
    public static final String USER_PROFILES_TOPIC   = "user-profiles";   // Compacted

    /**
     * Orders topic — 3 partitions for parallel processing.
     * Messages are keyed by userId so all events for a user go to one partition (ordering guarantee).
     * 7-day retention.
     */
    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name(ORDERS_TOPIC)
                .partitions(3)
                .replicas(1)   // Use 3 in production
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(7 * 24 * 60 * 60 * 1000L))
                .build();
    }

    /**
     * Orders Dead Letter Topic.
     * Receives messages that failed all retry attempts on the orders topic.
     */
    @Bean
    public NewTopic ordersDltTopic() {
        return TopicBuilder.name(ORDERS_DLT)
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(30L * 24 * 60 * 60 * 1000L)) // 30 days
                .build();
    }

    /**
     * Notifications topic — 3 partitions.
     * Demonstrates a second topic type in the system.
     */
    @Bean
    public NewTopic notificationsTopic() {
        return TopicBuilder.name(NOTIFICATIONS_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Notifications Dead Letter Topic.
     */
    @Bean
    public NewTopic notificationsDltTopic() {
        return TopicBuilder.name(NOTIFICATIONS_DLT)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * User Profiles topic — Compacted.
     * Only the latest profile event per userId is retained.
     * New consumers can bootstrap the current state of all users by reading this topic.
     */
    @Bean
    public NewTopic userProfilesTopic() {
        return TopicBuilder.name(USER_PROFILES_TOPIC)
                .partitions(6)
                .replicas(1)
                .compact()
                .build();
    }
}
