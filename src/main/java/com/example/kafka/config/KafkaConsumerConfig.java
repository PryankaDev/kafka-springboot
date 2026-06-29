package com.example.kafka.config;

import com.example.kafka.event.OrderEvent;
import com.example.kafka.event.NotificationEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Consumer Configuration
 *
 * Configures:
 *  - ConsumerFactory for OrderEvent and NotificationEvent
 *  - ConcurrentKafkaListenerContainerFactory (single and batch modes)
 *  - Error handler with exponential backoff + Dead Letter Topic (DLT)
 */
@Slf4j
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    // ── Base Consumer Properties ─────────────────────────────────────────────

    private Map<String, Object> baseConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);  // Manual ack
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        return props;
    }

    // ── ConsumerFactory: OrderEvent ──────────────────────────────────────────

    @Bean
    public ConsumerFactory<String, OrderEvent> orderConsumerFactory() {
        JsonDeserializer<OrderEvent> deserializer = new JsonDeserializer<>(OrderEvent.class);
        deserializer.addTrustedPackages("com.example.kafka.event");
        deserializer.setUseTypeMapperForKey(false);

        return new DefaultKafkaConsumerFactory<>(
                baseConsumerProps(),
                new StringDeserializer(),
                deserializer
        );
    }

    // ── ConsumerFactory: NotificationEvent ──────────────────────────────────

    @Bean
    public ConsumerFactory<String, NotificationEvent> notificationConsumerFactory() {
        JsonDeserializer<NotificationEvent> deserializer = new JsonDeserializer<>(NotificationEvent.class);
        deserializer.addTrustedPackages("com.example.kafka.event");

        return new DefaultKafkaConsumerFactory<>(
                baseConsumerProps(),
                new StringDeserializer(),
                deserializer
        );
    }

    // ── Error Handler: Exponential Backoff + DLT ─────────────────────────────

    /**
     * Error handler that:
     * 1. Retries with exponential backoff (1s → 2s → 4s, max 3 attempts)
     * 2. After all retries, publishes the message to the Dead Letter Topic
     * 3. Never retries deserialization or validation errors (permanent failures)
     */
    @Bean
    public CommonErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);

        // Exponential backoff: starts at 1s, doubles each attempt, max 3 retries
        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxAttempts(3);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

        // These exceptions go straight to DLT without any retry — retrying won't help
        handler.addNotRetryableExceptions(
                com.fasterxml.jackson.core.JsonParseException.class,
                javax.validation.ValidationException.class,
                IllegalArgumentException.class
        );

        handler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("Retry attempt {} for topic={}, partition={}, offset={}. Error: {}",
                        deliveryAttempt,
                        record.topic(), record.partition(), record.offset(),
                        ex.getMessage())
        );

        return handler;
    }

    // ── Listener Container Factory: Orders (Single Mode) ─────────────────────

    /**
     * Factory for single-record listeners.
     * Concurrency=3 means 3 consumer threads — one per partition.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> orderKafkaListenerContainerFactory(
            ConsumerFactory<String, OrderEvent> orderConsumerFactory,
            CommonErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(orderConsumerFactory);
        factory.setConcurrency(3);          // Match partition count
        factory.setCommonErrorHandler(errorHandler);
        factory.getContainerProperties().setAckMode(
                org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL
        );

        return factory;
    }

    // ── Listener Container Factory: Notifications (Batch Mode) ───────────────

    /**
     * Factory for batch listeners.
     * Collects up to max-poll-records messages before invoking the listener.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NotificationEvent> notificationKafkaListenerContainerFactory(
            ConsumerFactory<String, NotificationEvent> notificationConsumerFactory,
            CommonErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, NotificationEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(notificationConsumerFactory);
        factory.setBatchListener(true);     // Batch mode
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler);
        factory.getContainerProperties().setAckMode(
                org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL
        );

        return factory;
    }
}
