package com.rahul.notification.outbox;

import com.rahul.notification.entity.OutboxEvent;
import com.rahul.notification.entity.OutboxStatus;
import com.rahul.notification.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@Service
@RequiredArgsConstructor
public class OutboxClaimService {

    private static final long STALE_CLAIM_MINUTES = 2;

    private final OutboxEventRepository outboxEventRepository;
    private final PublisherInstanceIdentity instanceIdentity;

    @Transactional
    public List<OutboxEvent> claimBatch(int batchSize) {

        Instant now = Instant.now();

        Instant staleBefore = now.minus(STALE_CLAIM_MINUTES, ChronoUnit.MINUTES);

        List<OutboxEvent> events = outboxEventRepository.claimEligibleBatch(now, staleBefore, batchSize);

        for (OutboxEvent event : events) {

            event.setStatus(OutboxStatus.PROCESSING);

            event.setClaimedAt(now);

            event.setClaimedBy(instanceIdentity.getInstanceId());
        }

        return events;
    }
}