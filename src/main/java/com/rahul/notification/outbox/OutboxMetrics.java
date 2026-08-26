package com.rahul.notification.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class OutboxMetrics {

    private final Counter claimed;

    private final Counter published;

    private final Counter failed;

    private final Counter retried;

    private final Counter staleRecovered;

    public OutboxMetrics(MeterRegistry registry) {

        claimed = Counter.builder("outbox.events.claimed")
                .description("Number of outbox events claimed")
                .register(registry);

        published = Counter.builder("outbox.events.published")
                .description("Number of outbox events published")
                .register(registry);

        failed = Counter.builder("outbox.events.failed")
                .description("Number of outbox publishing failures")
                .register(registry);

        retried = Counter.builder("outbox.events.retried")
                .description("Number of outbox retries")
                .register(registry);

        staleRecovered = Counter.builder("outbox.events.stale.recovered")
                .description("Number of stale processing events recovered")
                .register(registry);
    }

    public void incrementClaimed() {
        claimed.increment();
    }

    public void incrementPublished() {
        published.increment();
    }

    public void incrementFailed() {
        failed.increment();
    }

    public void incrementRetried() {
        retried.increment();
    }

    public void incrementStaleRecovered() {
        staleRecovered.increment();
    }
}