/* package com.rahul.notification.integration;

import com.rahul.notification.event.UserRegisteredEvent;
import com.rahul.notification.event.UserRegisteredPayload;
import com.rahul.notification.repository.ProcessedEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@ActiveProfiles("test")
@Import(KafkaTestTopicConfig.class)
class IdempotencyIntegrationTest
        extends KafkaTestBase {

    @Autowired
    private KafkaTemplate<String, UserRegisteredEvent>
            kafkaTemplate;

    @Autowired
    private ProcessedEventRepository
            processedEventRepository;

    @Test
    void shouldProcessDuplicateEventOnlyOnce()
            throws Exception {

        String eventId =
                "integration-duplicate-" +
                        System.currentTimeMillis();

        UserRegisteredEvent event =
                new UserRegisteredEvent(
                        eventId,
                        "user.registered",
                        1,
                        Instant.now(),
                        "duplicate-user-001",
                        new UserRegisteredPayload(
                                "Rahul",
                                "duplicate@example.com",
                                "+919999999999"
                        )
                );

        kafkaTemplate
                .send(
                        "notifications.events",
                        "duplicate-user-001",
                        event
                )
                .get();

        kafkaTemplate
                .send(
                        "notifications.events",
                        "duplicate-user-001",
                        event
                )
                .get();

        await()
                .atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> {

                    long count =
                            processedEventRepository
                                    .findAll()
                                    .stream()
                                    .filter(e ->
                                            eventId.equals(
                                                    e.getEventId()
                                            )
                                    )
                                    .count();

                    assertThat(count)
                            .isEqualTo(1);
                });
    }
}
*/