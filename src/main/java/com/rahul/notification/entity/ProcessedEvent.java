package com.rahul.notification.entity;

import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "processed_events", uniqueConstraints = {@UniqueConstraint(name = "uk_processed_event_consumer", columnNames = {"event_id", "consumer_name"})})
public class ProcessedEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false)
    private String eventId;

    @Column(name = "consumer_name", nullable = false)
    private String consumerName;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    protected ProcessedEvent() {
    }

    public ProcessedEvent(String eventId, String consumerName, Instant processedAt) {

        this.eventId = eventId;
        this.consumerName = consumerName;
        this.processedAt = processedAt;
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}