package com.rahul.notification.outbox;

import com.rahul.notification.entity.OutboxEvent;
import com.rahul.notification.observability.CorrelationIdConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
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
    private final OutboxMetrics outboxMetrics;

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

        log.info("Publishing outbox event: eventId={}, correlationId={}, retryCount={}, claimedBy={}", event.getEventId(), event.getCorrelationId(), event.getRetryCount(), event.getClaimedBy());

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, event.getAggregateId(), event.getPayload());

        String correlationId = event.getCorrelationId();

        if (correlationId != null && !correlationId.isBlank()) {

            record.headers().add(CorrelationIdConstants.KAFKA_HEADER, correlationId.getBytes(StandardCharsets.UTF_8));
        }

        try {

            SendResult<String, String> result = outboxKafkaTemplate.send(record).get(6, TimeUnit.SECONDS);

            outboxMetrics.incrementPublished();

            log.info("OUTBOX KAFKA SEND SUCCESS: eventId={}, correlationId={}, partition={}, offset={}", event.getEventId(), correlationId, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());

            stateService.markPublished(event.getId());

        } catch (Exception exception) {
            outboxMetrics.incrementFailed();
            log.error("OUTBOX KAFKA SEND FAILED: eventId={}, correlationId={}, type={}, message={}", event.getEventId(), correlationId, exception.getClass().getName(), exception.getMessage(), exception);

            stateService.markPublishFailed(event.getId(), exception);
        }
    }
}