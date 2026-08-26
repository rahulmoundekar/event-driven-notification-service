package com.rahul.notification.integration;

import com.rahul.notification.event.UserRegisteredEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class KafkaIntegrationTestSupport {

    private KafkaIntegrationTestSupport() {
    }

    public static Consumer<String, UserRegisteredEvent> createConsumer(String bootstrapServers, String groupId) {

        Map<String, Object> properties = new HashMap<>();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        JacksonJsonDeserializer<UserRegisteredEvent> deserializer = new JacksonJsonDeserializer<>(UserRegisteredEvent.class);

        deserializer.addTrustedPackages("com.rahul.notification.event");

        return new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties, new StringDeserializer(), deserializer);
    }

    public static ConsumerRecord<String, UserRegisteredEvent> waitForEvent(Consumer<String, UserRegisteredEvent> consumer, String topic, String expectedEventId, Duration timeout) {

        consumer.subscribe(Collections.singletonList(topic));

        long deadline = System.nanoTime() + timeout.toNanos();

        while (System.nanoTime() < deadline) {

            var records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<String, UserRegisteredEvent> record : records) {

                UserRegisteredEvent event = record.value();

                if (event != null && expectedEventId.equals(event.eventId())) {

                    return record;
                }
            }
        }

        throw new AssertionError("Event not found in topic=" + topic + ", eventId=" + expectedEventId);
    }
}