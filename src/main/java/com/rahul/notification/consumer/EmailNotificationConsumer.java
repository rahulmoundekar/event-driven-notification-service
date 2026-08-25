package com.rahul.notification.consumer;

import com.rahul.notification.event.UserRegisteredEvent;
import com.rahul.notification.exception.EmailDeliveryException;
import com.rahul.notification.exception.InvalidNotificationException;
import com.rahul.notification.service.IdempotencyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class EmailNotificationConsumer {

    private static final String CONSUMER_NAME = "email-service";

    private final IdempotencyService idempotencyService;

    @KafkaListener(topics = "notifications.events", groupId = "email-service", containerFactory = "kafkaListenerContainerFactory")
    public void consume(UserRegisteredEvent event) {

        log.info("Received event: eventId={}, userId={}, email={}", event.eventId(), event.userId(), event.payload().email());

        if (idempotencyService.alreadyProcessed(event.eventId(), CONSUMER_NAME)) {

            log.warn("Duplicate event detected. Skipping eventId={}", event.eventId());

            return;
        }

        sendWelcomeEmail(event);

        idempotencyService.markProcessed(event.eventId(), CONSUMER_NAME);

        log.info("Event processed successfully: eventId={}", event.eventId());
    }

    private void sendWelcomeEmail(UserRegisteredEvent event) {

        String email = event.payload().email();

        if ("invalid@example.com".equalsIgnoreCase(email)) {

            throw new InvalidNotificationException("Invalid email address");
        }

        if ("fail@example.com".equalsIgnoreCase(email)) {

            throw new EmailDeliveryException("Email provider temporarily unavailable");
        }

        log.info("Sending welcome email to {}", email);
    }
}