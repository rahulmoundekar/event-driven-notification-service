package com.rahul.notification.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    public static final String NOTIFICATIONS_TOPIC =
            "notifications.events";

    public static final String NOTIFICATIONS_DLT =
            "notifications.events.DLT";

    @Bean
    public NewTopic notificationsEventsTopic() {

        return new NewTopic(
                NOTIFICATIONS_TOPIC,
                3,
                (short) 1
        );
    }

    @Bean
    public NewTopic notificationsEventsDltTopic() {

        return new NewTopic(
                NOTIFICATIONS_DLT,
                3,
                (short) 1
        );
    }
}