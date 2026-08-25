package com.rahul.notification.event;

import java.time.Instant;

public record UserRegisteredEvent(
        String eventId,
        String eventType,
        int eventVersion,
        Instant occurredAt,
        String userId,
        UserRegisteredPayload payload
) {
}