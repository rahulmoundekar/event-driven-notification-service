package com.rahul.notification.outbox;

import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class OutboxRetryPolicy {

    private static final int MAX_RETRIES = 3;

    private static final long INITIAL_DELAY_SECONDS = 2;

    private static final long MAX_DELAY_SECONDS = 30;

    public boolean canRetry(int retryCount) {
        return retryCount < MAX_RETRIES;
    }

    public Duration nextDelay(int retryCount) {

        long delay = INITIAL_DELAY_SECONDS * (1L << retryCount);

        delay = Math.min(delay, MAX_DELAY_SECONDS);

        return Duration.ofSeconds(delay);
    }

    public int maxRetries() {
        return MAX_RETRIES;
    }
}