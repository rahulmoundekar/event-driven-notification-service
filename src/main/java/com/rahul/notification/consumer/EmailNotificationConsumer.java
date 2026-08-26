package com.rahul.notification.consumer;

import com.rahul.notification.event.EventTypes;
import com.rahul.notification.event.UserRegisteredEvent;
import com.rahul.notification.exception.EmailDeliveryException;
import com.rahul.notification.exception.InvalidNotificationException;
import com.rahul.notification.exception.UnsupportedEventVersionException;
import com.rahul.notification.observability.CorrelationIdConstants;
import com.rahul.notification.observability.NotificationMetrics;
import com.rahul.notification.service.IdempotencyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class EmailNotificationConsumer {

    private static final String CONSUMER_NAME = "email-service";

    private final IdempotencyService idempotencyService;
    private final NotificationMetrics notificationMetrics;

    @KafkaListener(topics = "notifications.events", groupId = "email-service", containerFactory = "kafkaListenerContainerFactory")
    @Transactional
    public void consume(UserRegisteredEvent event, @Header(name = CorrelationIdConstants.KAFKA_HEADER, required = false) String correlationId) {

        MDC.put(CorrelationIdConstants.MDC_KEY, correlationId);

        log.info("Received event: eventId={}, type={}, version={}, correlationId={}, userId={}", event.eventId(), event.eventType(), event.eventVersion(), correlationId, event.aggregateId());

        validateEvent(event);
        try {
            if (idempotencyService.alreadyProcessed(event.eventId(), CONSUMER_NAME)) {

                log.warn("Duplicate event detected. Skipping eventId={}", event.eventId());
                notificationMetrics.incrementDuplicate();
                return;
            }

            switch (event.eventVersion()) {

                case 1 -> handleV1(event);

                default ->
                        throw new UnsupportedEventVersionException("Unsupported USER_REGISTERED event version: " + event.eventVersion());
            }

            log.info(
                    "Marking event as processed: eventId={}, consumer={}",
                    event.eventId(),
                    CONSUMER_NAME
            );


            idempotencyService.markProcessed(event.eventId(), CONSUMER_NAME);

            log.info(
                    "Event marked as processed: eventId={}, consumer={}",
                    event.eventId(),
                    CONSUMER_NAME
            );

            notificationMetrics.incrementProcessed();
        } finally {
            MDC.remove(CorrelationIdConstants.MDC_KEY);
        }
    }

    private void handleV1(UserRegisteredEvent event) {

        if (!EventTypes.USER_REGISTERED.equals(event.eventType())) {

            throw new InvalidNotificationException("Unsupported event type: " + event.eventType());
        }

        sendWelcomeEmail(event);
    }

    private void validateEvent(UserRegisteredEvent event) {

        if (event.eventId() == null || event.eventId().isBlank()) {

            throw new InvalidNotificationException("eventId is required");
        }

        if (event.eventType() == null || event.eventType().isBlank()) {

            throw new InvalidNotificationException("eventType is required");
        }

        if (event.aggregateId() == null || event.aggregateId().isBlank()) {

            throw new InvalidNotificationException("aggregateId is required");
        }

        if (event.payload() == null) {

            throw new InvalidNotificationException("payload is required");
        }
    }

    /*private void sendWelcomeEmail(UserRegisteredEvent event) {

        String email = event.payload().email();

        if ("invalid@example.com".equalsIgnoreCase(email)) {

            throw new InvalidNotificationException("Invalid email address");
        }

        if ("fail@example.com".equalsIgnoreCase(email)) {

            throw new EmailDeliveryException("Email provider temporarily unavailable");
        }

        log.info("Sending welcome email to {}", email);
    }*/

    private void sendWelcomeEmail(UserRegisteredEvent event) {

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Notification interrupted",
                    e
            );
        }

        log.info(
                "Sending welcome email to {}",
                event.payload().email()
        );
    }
}