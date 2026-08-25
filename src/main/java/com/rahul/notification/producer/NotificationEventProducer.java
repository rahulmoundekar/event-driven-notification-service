package com.rahul.notification.producer;

import com.rahul.notification.event.UserRegisteredEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
public class NotificationEventProducer {

    private static final String TOPIC = "notifications.events";

    private final KafkaTemplate<String, UserRegisteredEvent> kafkaTemplate;

    public CompletableFuture<?> publishUserRegisteredEvent(
            UserRegisteredEvent event) {

        return kafkaTemplate
                .send(TOPIC, event.userId(), event)
                .thenAccept(result -> {

                    var metadata = result.getRecordMetadata();

                    System.out.println(
                            "Event published successfully: " +
                                    "topic=" + metadata.topic() +
                                    ", partition=" + metadata.partition() +
                                    ", offset=" + metadata.offset()
                    );
                })
                .exceptionally(ex -> {

                    System.err.println(
                            "Failed to publish event: "
                                    + ex.getMessage()
                    );

                    throw new RuntimeException(
                            "Kafka publishing failed",
                            ex
                    );
                });
    }
}