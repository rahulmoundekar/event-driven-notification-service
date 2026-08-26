package com.rahul.notification.integration;

import com.rahul.notification.event.UserRegisteredEvent;
import com.rahul.notification.event.UserRegisteredPayload;
import com.rahul.notification.repository.ProcessedEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@AutoConfigureMockMvc
class RetryAndDltIntegrationTest
        extends KafkaTestBase {

    @Autowired
    private KafkaTemplate<String, UserRegisteredEvent>
            kafkaTemplate;

    @Autowired
    private ProcessedEventRepository
            processedEventRepository;

    @Test
    void retryableEmailFailureShouldEventuallyGoToDlt()
            throws Exception {

        String eventId =
                "integration-retry-001";

        UserRegisteredEvent event =
                new UserRegisteredEvent(
                        eventId,
                        "user.registered",
                        1,
                        Instant.now(),
                        "retry-user-001",
                        new UserRegisteredPayload(
                                "Retry Test",
                                "fail@example.com",
                                "+919999999999"
                        )
                );

        kafkaTemplate
                .send(
                        "notifications.events",
                        "retry-user-001",
                        event
                )
                .get();

        try (
                var dltConsumer =
                        KafkaIntegrationTestSupport.createConsumer(
                                kafka.getBootstrapServers(),
                                "integration-retry-dlt-" +
                                        eventId
                        )
        ) {

            var record =
                    KafkaIntegrationTestSupport.waitForEvent(
                            dltConsumer,
                            "notifications.events.DLT",
                            eventId,
                            Duration.ofSeconds(30)
                    );

            assertThat(record.value().eventId())
                    .isEqualTo(eventId);
        }

        assertThat(
                processedEventRepository
                        .existsByEventIdAndConsumerName(
                                eventId,
                                "email-service"
                        )
        ).isFalse();
    }
}