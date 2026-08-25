package com.rahul.notification.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@Entity
@Table(
        name = "outbox_events",
        indexes = {
                @Index(
                        name = "idx_outbox_status_created",
                        columnList = "status, created_at"
                )
        }
)
@Getter
@Setter

public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(
            name = "event_id",
            nullable = false,
            unique = true
    )
    private String eventId;

    @Column(
            name = "event_type",
            nullable = false
    )
    private String eventType;

    @Column(
            name = "aggregate_type",
            nullable = false
    )
    private String aggregateType;

    @Column(
            name = "aggregate_id",
            nullable = false
    )
    private String aggregateId;

    @Column(
            name = "payload",
            nullable = false,
            columnDefinition = "TEXT"
    )
    private String payload;

    @Column(
            name = "status",
            nullable = false
    )
    @Enumerated(EnumType.STRING)
    private OutboxStatus status;

    @Column(
            name = "created_at",
            nullable = false
    )
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    // Retry information
    @Column(
            name = "retry_count",
            nullable = false
    )
    private int retryCount;

    @Column(name = "next_attempt_at")
    private Instant nextAttemptAt;

    @Column(name = "last_error")
    private String lastError;

    @Column(name = "claimed_at")
    private Instant claimedAt;

    @Column(name = "claimed_by")
    private String claimedBy;

    protected OutboxEvent() {
    }

    public OutboxEvent(
            String eventId,
            String eventType,
            String aggregateType,
            String aggregateId,
            String payload) {

        this.eventId = eventId;
        this.eventType = eventType;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.payload = payload;

        this.status = OutboxStatus.NEW;
        this.createdAt = Instant.now();

        this.retryCount = 0;
        this.nextAttemptAt = null;
        this.lastError = null;

        this.claimedAt = null;
        this.claimedBy = null;
    }

    // getters and setters
}