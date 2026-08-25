package com.rahul.notification.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class NotificationMetrics {

    private final Counter processedCounter;
    private final Counter duplicateCounter;
    private final Counter failedCounter;

    public NotificationMetrics(MeterRegistry registry) {

        this.processedCounter = Counter.builder("notification.processed").description("Successfully processed notifications").register(registry);

        this.duplicateCounter = Counter.builder("notification.duplicate").description("Duplicate notification events").register(registry);

        this.failedCounter = Counter.builder("notification.failed").description("Failed notification processing").register(registry);
    }

    public void incrementProcessed() {
        processedCounter.increment();
    }

    public void incrementDuplicate() {
        duplicateCounter.increment();
    }

    public void incrementFailed() {
        failedCounter.increment();
    }
}