package com.rahul.notification.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class NotificationMetrics {

    private final Counter processed;

    private final Counter duplicate;

    private final Counter failed;

    private final Counter unsupportedVersion;

    public NotificationMetrics(
            MeterRegistry registry) {

        processed =
                Counter.builder("notification.events.processed")
                        .description("Successfully processed notification events")
                        .register(registry);

        duplicate =
                Counter.builder("notification.events.duplicate")
                        .description("Duplicate notification events")
                        .register(registry);

        failed =
                Counter.builder("notification.events.failed")
                        .description("Notification processing failures")
                        .register(registry);

        unsupportedVersion =
                Counter.builder("notification.events.unsupported.version")
                        .description("Unsupported notification event versions")
                        .register(registry);
    }

    public void incrementProcessed() {
        processed.increment();
    }

    public void incrementDuplicate() {
        duplicate.increment();
    }

    public void incrementFailed() {
        failed.increment();
    }

    public void incrementUnsupportedVersion() {
        unsupportedVersion.increment();
    }
}