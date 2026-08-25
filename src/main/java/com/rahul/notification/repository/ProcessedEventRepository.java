package com.rahul.notification.repository;

import com.rahul.notification.entity.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository
        extends JpaRepository<ProcessedEvent, Long> {

    boolean existsByEventIdAndConsumerName(
            String eventId,
            String consumerName
    );
}