package com.rahul.notification.outbox;

import com.rahul.notification.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class OutboxStateService {

    private final OutboxEventRepository outboxRepository;
    private final OutboxRetryPolicy retryPolicy;
    private final OutboxMetrics outboxMetrics;

    @Transactional
    public void markPublished(Long outboxId) {

        outboxRepository.markPublished(outboxId, Instant.now());
    }

    @Transactional
    public void markPublishFailed(Long outboxId, Throwable exception) {

        // Read current count only to calculate the next delay.
        var event = outboxRepository.findById(outboxId).orElseThrow();

        int currentRetry = event.getRetryCount();

        if (!retryPolicy.canRetry(currentRetry)) {

            outboxRepository.markFailed(outboxId, exception.getMessage(), null);

            return;
        }

        Instant nextAttempt = Instant.now().plus(retryPolicy.nextDelay(currentRetry));
        outboxMetrics.incrementRetried();
        outboxRepository.markFailed(outboxId, exception.getMessage(), nextAttempt);
    }
}