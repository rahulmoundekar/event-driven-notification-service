package com.rahul.notification.integration;

import org.junit.jupiter.api.BeforeAll;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public abstract class KafkaTestBase {

    @Container
    protected static final PostgreSQLContainer postgres =
            new PostgreSQLContainer(
                    DockerImageName.parse("postgres:17")
            )
                    .withDatabaseName("notification_test")
                    .withUsername("test")
                    .withPassword("test");

    @Container
    protected static final KafkaContainer kafka =
            new KafkaContainer(
                    DockerImageName.parse("apache/kafka:4.2.1")
            );

    @DynamicPropertySource
    static void registerProperties(
            DynamicPropertyRegistry registry) {

        // PostgreSQL
        registry.add(
                "spring.datasource.url",
                postgres::getJdbcUrl
        );

        registry.add(
                "spring.datasource.username",
                postgres::getUsername
        );

        registry.add(
                "spring.datasource.password",
                postgres::getPassword
        );

        registry.add(
                "spring.jpa.hibernate.ddl-auto",
                () -> "create-drop"
        );

        // Kafka
        registry.add(
                "spring.kafka.bootstrap-servers",
                kafka::getBootstrapServers
        );
    }

    @BeforeAll
    static void printInfrastructure() {

        System.out.println(
                "TEST POSTGRES = " +
                        postgres.getJdbcUrl()
        );

        System.out.println(
                "TEST KAFKA = " +
                        kafka.getBootstrapServers()
        );
    }
}