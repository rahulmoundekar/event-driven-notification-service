package com.rahul.notification.service;

import com.rahul.notification.entity.ProcessedEvent;
import com.rahul.notification.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final ProcessedEventRepository repository;

    public boolean alreadyProcessed(
            String eventId,
            String consumerName) {

        return repository.existsByEventIdAndConsumerName(
                eventId,
                consumerName
        );
    }

    public void markProcessed(
            String eventId,
            String consumerName) {

        ProcessedEvent processedEvent =
                new ProcessedEvent(
                        eventId,
                        consumerName,
                        Instant.now()
                );

        ProcessedEvent saved =
                repository.saveAndFlush(processedEvent);

        log.info(
                "PROCESSED_EVENT SAVED: id={}, eventId={}, consumer={}",
                saved.getId(),
                saved.getEventId(),
                saved.getConsumerName()
        );
    }
}