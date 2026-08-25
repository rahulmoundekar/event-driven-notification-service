package com.rahul.notification.consumer;
import com.rahul.notification.event.UserRegisteredEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AuditNotificationConsumer {

    @KafkaListener(
            topics = "notifications.events",
            groupId = "audit-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(UserRegisteredEvent event) {

        log.info(
                "AUDIT CONSUMER received eventId={}",
                event.eventId()
        );
    }
}
