package com.rahul.notification.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rahul.notification.event.UserRegisteredEvent;
import com.rahul.notification.event.UserRegisteredPayload;
import com.rahul.notification.repository.ProcessedEventRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class EventVersionIntegrationTest extends KafkaTestBase {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    private final ObjectMapper objectMapper =
            new ObjectMapper()
                    .registerModule(new JavaTimeModule());


    @Test
    void supportedV1ShouldBeProcessed()
            throws Exception {

        String eventId =
                "integration-version-v1-001";

        UserRegisteredEvent event =
                new UserRegisteredEvent(
                        eventId,
                        "user.registered",
                        1,
                        Instant.now(),
                        "version-user-001",
                        new UserRegisteredPayload(
                                "Rahul",
                                "v1@example.com",
                                "+919999999999"
                        )
                );

        String eventJson =
                objectMapper.writeValueAsString(event);
        kafkaTemplate
                .send(
                        "notifications.events",
                        "version-user-001",
                        eventJson
                )
                .get();

        try (
                Consumer<String, UserRegisteredEvent> consumer =
                        KafkaIntegrationTestSupport.createConsumer(
                                kafka.getBootstrapServers(),
                                "integration-version-observer-" + eventId
                        )
        ) {

            var record =
                    KafkaIntegrationTestSupport.waitForEvent(
                            consumer,
                            "notifications.events",
                            eventId,
                            Duration.ofSeconds(20)
                    );

            assertThat(record.value().eventId())
                    .isEqualTo(eventId);

            assertThat(record.value().eventType())
                    .isEqualTo("user.registered");

            assertThat(record.value().eventVersion())
                    .isEqualTo(1);

            assertThat(record.value().aggregateId())
                    .isEqualTo("version-user-001");
        }
    }

  /*  @Test
    void supportedV1ShouldBeProcessed()
            throws Exception {

        String eventId =
                "integration-version-v1-001";

        UserRegisteredEvent event =
                new UserRegisteredEvent(
                        eventId,
                        "user.registered",
                        1,
                        Instant.now(),
                        "version-user-001",
                        new UserRegisteredPayload(
                                "Rahul",
                                "v1@example.com",
                                "+919999999999"
                        )
                );

        String eventJson =
                objectMapper.writeValueAsString(event);

        kafkaTemplate
                .send(
                        "notifications.events",
                        "version-user-001",
                        eventJson
                )
                .get();

        org.awaitility.Awaitility.await()
                .atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> {

                    assertThat(
                            processedEventRepository
                                    .existsByEventIdAndConsumerName(
                                            eventId,
                                            "email-service"
                                    )
                    ).isTrue();
                });
    }*/

    @Test
    void unsupportedVersionShouldGoToDlt()
            throws Exception {

        String eventId =
                "integration-version-v999-001";

        UserRegisteredEvent event =
                new UserRegisteredEvent(
                        eventId,
                        "user.registered",
                        999,
                        Instant.now(),
                        "version-user-999",
                        new UserRegisteredPayload(
                                "Rahul",
                                "v999@example.com",
                                "+919999999999"
                        )
                );

        String eventJson =
                objectMapper.writeValueAsString(event);

        kafkaTemplate
                .send(
                        "notifications.events",
                        "version-user-999",
                        eventJson
                )
                .get();

        try (
                Consumer<String, UserRegisteredEvent> dltConsumer =
                        KafkaIntegrationTestSupport.createConsumer(
                                kafka.getBootstrapServers(),
                                "integration-version-dlt-" + eventId
                        )
        ) {

            var record =
                    KafkaIntegrationTestSupport.waitForEvent(
                            dltConsumer,
                            "notifications.events.DLT",
                            eventId,
                            Duration.ofSeconds(20)
                    );

            assertThat(record.value().eventId())
                    .isEqualTo(eventId);

            assertThat(record.value().eventVersion())
                    .isEqualTo(999);
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