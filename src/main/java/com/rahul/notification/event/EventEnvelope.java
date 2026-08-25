package com.rahul.notification.event;

import java.time.Instant;

public record EventEnvelope<T>(
        String eventId,
        String eventType,
        int eventVersion,
        Instant occurredAt,
        String aggregateId,
        T payload
) {
}