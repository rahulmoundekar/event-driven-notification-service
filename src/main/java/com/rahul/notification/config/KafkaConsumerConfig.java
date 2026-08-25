package com.rahul.notification.config;

import com.rahul.notification.event.UserRegisteredEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, UserRegisteredEvent> consumerFactory(
            @Value("${spring.kafka.bootstrap-servers}")
            String bootstrapServers) {

        Map<String, Object> properties = new HashMap<>();

        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                JacksonJsonDeserializer.class
        );

        properties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        JacksonJsonDeserializer<UserRegisteredEvent> deserializer =
                new JacksonJsonDeserializer<>(UserRegisteredEvent.class);

        deserializer.addTrustedPackages(
                "com.rahul.notification.event"
        );

        return new DefaultKafkaConsumerFactory<>(
                properties,
                new StringDeserializer(),
                deserializer
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, UserRegisteredEvent>
    kafkaListenerContainerFactory(
            ConsumerFactory<String, UserRegisteredEvent> consumerFactory,
            CommonErrorHandler kafkaErrorHandler) {

        var factory =
                new ConcurrentKafkaListenerContainerFactory<String, UserRegisteredEvent>();

        factory.setConsumerFactory(consumerFactory);

        factory.setCommonErrorHandler(kafkaErrorHandler);

        return factory;
    }
}