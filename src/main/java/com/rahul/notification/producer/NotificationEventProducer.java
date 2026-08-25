package com.rahul.notification.producer;

import com.rahul.notification.event.UserRegisteredEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationEventProducer {

    private static final String TOPIC = "notifications.events";

    private final KafkaTemplate<String, UserRegisteredEvent> kafkaTemplate;

    public CompletableFuture<?> publishUserRegisteredEvent(
            UserRegisteredEvent event) {

        return kafkaTemplate
                .send(TOPIC, event.aggregateId(), event)
                .thenAccept(result -> {

                    var metadata = result.getRecordMetadata();

                    log.info("Event published successfully: topic={}, partition={}, offset={}", metadata.topic(), metadata.partition(), metadata.offset());
                })
                .exceptionally(ex -> {

                    log.error("Failed to publish event: {}", ex.getMessage());

                    throw new RuntimeException(
                            "Kafka publishing failed",
                            ex
                    );
                });
    }
}