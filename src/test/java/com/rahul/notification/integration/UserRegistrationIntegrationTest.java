package com.rahul.notification.integration;

import com.rahul.notification.entity.OutboxEvent;
import com.rahul.notification.entity.OutboxStatus;
import com.rahul.notification.repository.OutboxEventRepository;
import com.rahul.notification.repository.ProcessedEventRepository;
import com.rahul.notification.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class UserRegistrationIntegrationTest extends KafkaTestBase {

    @Autowired
    MockMvc mockMvc;

    @Autowired
    UserRepository userRepository;

    @Autowired
    OutboxEventRepository outboxEventRepository;

    @Autowired
    ProcessedEventRepository processedEventRepository;

    @Test
    void shouldCreateUserAndOutboxEvent() throws Exception {

        mockMvc.perform(post("/api/users").contentType("application/json").header("X-Correlation-Id", "TEST-13B-001").content("""
                {
                  "userId": "test-user-13b-001",
                  "name": "Rahul",
                  "email": "test13b@example.com",
                  "phone": "+919999999999"
                }
                """)).andExpect(status().isOk());

        assertThat(userRepository.existsById("test-user-13b-001")).isTrue();

        OutboxEvent event = outboxEventRepository.findAll().stream().filter(e -> "test-user-13b-001".equals(e.getAggregateId())).findFirst().orElseThrow();

        assertThat(event.getEventType()).isEqualTo("USER_REGISTERED");

        assertThat(event.getCorrelationId()).isEqualTo("TEST-13B-001");
    }

    @Test
    void shouldPublishOutboxEventToKafkaAndProcessIt() throws Exception {

        mockMvc.perform(post("/api/users").contentType("application/json").header("X-Correlation-Id", "TEST-13B-001").content("""
                {
                  "userId": "test-user-13b-001",
                  "name": "Rahul",
                  "email": "test13b@example.com",
                  "phone": "+919999999999"
                }
                """)).andExpect(status().isOk());

        assertThat(userRepository.existsById("test-user-13b-001")).isTrue();

        OutboxEvent event = outboxEventRepository.findAll().stream().filter(e -> "test-user-13b-001".equals(e.getAggregateId())).findFirst().orElseThrow();

        assertThat(event.getCorrelationId()).isEqualTo("TEST-13B-001");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {

            OutboxEvent current = outboxEventRepository.findById(event.getId()).orElseThrow();

            assertThat(current.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        });

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {

            assertThat(processedEventRepository.existsByEventIdAndConsumerName(event.getEventId(), "email-service")).isTrue();
        });
    }
}