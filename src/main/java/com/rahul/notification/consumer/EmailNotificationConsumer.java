package com.rahul.notification.consumer;

import com.rahul.notification.event.UserRegisteredEvent;
import com.rahul.notification.exception.EmailDeliveryException;
import com.rahul.notification.exception.InvalidNotificationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class EmailNotificationConsumer {

    @KafkaListener(topics = "notifications.events", groupId = "email-service", containerFactory = "kafkaListenerContainerFactory")
    public void consume(UserRegisteredEvent event) {

        log.info("Received UserRegisteredEvent: eventId={}, userId={}, email={}", event.eventId(), event.userId(), event.payload().email());

        sendWelcomeEmail(event);
    }

    private void sendWelcomeEmail(UserRegisteredEvent event) {

        String email = event.payload().email();

        // Permanent failure
        if ("invalid@example.com".equalsIgnoreCase(email)) {

            log.error("Invalid notification data. eventId={}", event.eventId());

            throw new InvalidNotificationException("Invalid email address");
        }

        // Temporary failure
        if ("fail@example.com".equalsIgnoreCase(email)) {

            log.error("Temporary email provider failure. eventId={}", event.eventId());

            throw new EmailDeliveryException("Email provider temporarily unavailable");
        }

        log.info("Sending welcome email to {}", email);
    }
}