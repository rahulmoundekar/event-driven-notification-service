package com.rahul.notification.repository;

import com.rahul.notification.entity.OutboxEvent;
import com.rahul.notification.entity.OutboxStatus;
import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    @Query(value = """
            SELECT *
            FROM outbox_events
            WHERE
                status = 'NEW'
                OR (
                    status = 'FAILED'
                    AND next_attempt_at IS NOT NULL
                    AND next_attempt_at <= :now
                )
                OR (
                    status = 'PROCESSING'
                    AND claimed_at IS NOT NULL
                    AND claimed_at <= :staleBefore
                )
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT :limit
            """, nativeQuery = true)
    List<OutboxEvent> claimEligibleBatch(@Param("now") Instant now, @Param("staleBefore") Instant staleBefore, @Param("limit") int limit);


    @Modifying
    @Query(
            value = """
                UPDATE outbox_events
                SET
                    retry_count = retry_count + 1,
                    last_error = :lastError,
                    status = 'FAILED',
                    next_attempt_at = :nextAttemptAt,
                    claimed_at = NULL,
                    claimed_by = NULL
                WHERE id = :id
                """,
            nativeQuery = true
    )
    int markFailed(
            @Param("id") Long id,
            @Param("lastError") String lastError,
            @Param("nextAttemptAt") Instant nextAttemptAt
    );

    @Modifying
    @Query(
            value = """
                UPDATE outbox_events
                SET
                    status = 'PUBLISHED',
                    published_at = :publishedAt,
                    claimed_at = NULL,
                    claimed_by = NULL,
                    next_attempt_at = NULL
                WHERE id = :id
                """,
            nativeQuery = true
    )
    int markPublished(
            @Param("id") Long id,
            @Param("publishedAt") Instant publishedAt
    );
}