package com.rahul.notification.outbox;

import com.rahul.notification.entity.OutboxEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPublisher {

    private static final String TOPIC = "notifications.events";

    private final OutboxClaimService claimService;
    private final OutboxStateService stateService;
    private final KafkaTemplate<String, String> outboxKafkaTemplate;

    @Scheduled(fixedDelay = 1000)
    public void publishEvents() {

        List<OutboxEvent> events = claimService.claimBatch(1);

        if (events.isEmpty()) {
            return;
        }

        log.info("Claimed {} outbox events", events.size());

        events.forEach(this::publish);
    }

    private void publish(OutboxEvent event) {

        log.info("Publishing outbox event: eventId={}, retryCount={}, claimedBy={}", event.getEventId(), event.getRetryCount(), event.getClaimedBy());

        try {

            SendResult<String, String> result = outboxKafkaTemplate.send(TOPIC, event.getAggregateId(), event.getPayload()).get(6, TimeUnit.SECONDS);

            log.info("OUTBOX KAFKA SEND SUCCESS: eventId={}, partition={}, offset={}", event.getEventId(), result.getRecordMetadata().partition(), result.getRecordMetadata().offset());

            stateService.markPublished(event.getId());

        } catch (Exception exception) {

            log.error("OUTBOX KAFKA SEND FAILED: eventId={}, type={}, message={}", event.getEventId(), exception.getClass().getName(), exception.getMessage(), exception);

            stateService.markPublishFailed(event.getId(), exception);
        }
    }

    private void handleResult(OutboxEvent event, SendResult<String, String> result, Throwable exception) {

        if (exception != null) {

            log.error("OUTBOX KAFKA SEND FAILED: eventId={}, errorType={}, message={}", event.getEventId(), exception.getClass().getName(), exception.getMessage(), exception);

            stateService.markPublishFailed(event.getId(), exception);

            return;
        }

        log.info("OUTBOX KAFKA SEND SUCCESS: eventId={}, partition={}, offset={}", event.getEventId(), result.getRecordMetadata().partition(), result.getRecordMetadata().offset());

        stateService.markPublished(event.getId());
    }
}