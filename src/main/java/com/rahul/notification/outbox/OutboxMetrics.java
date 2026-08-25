package com.rahul.notification.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class OutboxMetrics {

    private final Counter publishedCounter;
    private final Counter failedCounter;
    private final Counter retryCounter;

    public OutboxMetrics(MeterRegistry registry) {

        this.publishedCounter = Counter.builder("outbox.published").description("Successfully published outbox events").register(registry);

        this.failedCounter = Counter.builder("outbox.failed").description("Outbox publishing failures").register(registry);

        this.retryCounter = Counter.builder("outbox.retry").description("Outbox retry attempts").register(registry);
    }

    public void incrementPublished() {
        publishedCounter.increment();
    }

    public void incrementFailed() {
        failedCounter.increment();
    }

    public void incrementRetry() {
        retryCounter.increment();
    }
}