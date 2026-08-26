package com.rahul.notification.integration;

import com.rahul.notification.entity.OutboxEvent;
import com.rahul.notification.entity.OutboxStatus;
import com.rahul.notification.repository.OutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class ObservabilityIntegrationTest extends KafkaTestBase {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Test
    void correlationIdShouldBePersistedAndPublished()
            throws Exception {

        String userId = "correlation-test-001";
        String correlationId = "INTEGRATION-CORR-001";

        mockMvc.perform(
                        post("/api/users")
                                .contentType("application/json")
                                .header(
                                        "X-Correlation-Id",
                                        correlationId
                                )
                                .content("""
                                {
                                  "userId": "correlation-test-001",
                                  "name": "Correlation Test",
                                  "email": "correlation@example.com",
                                  "phone": "+919999999999"
                                }
                                """)
                )
                .andExpect(status().isOk());

        OutboxEvent event =
                await()
                        .atMost(Duration.ofSeconds(10))
                        .until(
                                () -> outboxEventRepository
                                        .findAll()
                                        .stream()
                                        .filter(e ->
                                                userId.equals(
                                                        e.getAggregateId()
                                                )
                                        )
                                        .findFirst()
                                        .orElse(null),
                                Objects::nonNull
                        );

        assertThat(event.getCorrelationId())
                .isEqualTo(correlationId);

        await()
                .atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> {

                    OutboxEvent current =
                            outboxEventRepository
                                    .findById(event.getId())
                                    .orElseThrow();

                    assertThat(current.getStatus())
                            .isEqualTo(
                                    OutboxStatus.PUBLISHED
                            );
                });
    }
}