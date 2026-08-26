package com.rahul.notification.integration;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@TestConfiguration
public class KafkaTestTopicConfig {

    @Bean
    NewTopic notificationsEventsTopic() {

        return new NewTopic(
                "notifications.events",
                1,
                (short) 1
        );
    }

    @Bean
    NewTopic notificationsEventsDltTopic() {

        return new NewTopic(
                "notifications.events.DLT",
                1,
                (short) 1
        );
    }
}