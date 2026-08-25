package com.rahul.notification.service;

import com.rahul.notification.entity.ProcessedEvent;
import com.rahul.notification.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

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

    @Transactional
    public void markProcessed(
            String eventId,
            String consumerName) {

        repository.save(
                new ProcessedEvent(
                        eventId,
                        consumerName,
                        Instant.now()
                )
        );
    }
}