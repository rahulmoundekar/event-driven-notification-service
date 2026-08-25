package com.rahul.notification.config;

import com.rahul.notification.event.UserRegisteredEvent;
import com.rahul.notification.exception.InvalidNotificationException;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaConsumerErrorHandlerConfig {

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<String, UserRegisteredEvent> kafkaTemplate) {

        return new DeadLetterPublishingRecoverer(kafkaTemplate, (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
    }

    @Bean
    public CommonErrorHandler kafkaErrorHandler(DeadLetterPublishingRecoverer recoverer) {

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(2);

        backOff.setInitialInterval(1000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(5000L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        errorHandler.addRetryableExceptions(com.rahul.notification.exception.EmailDeliveryException.class);

        errorHandler.addNotRetryableExceptions(InvalidNotificationException.class);

        return errorHandler;
    }
}