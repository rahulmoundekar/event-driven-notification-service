# Event-Driven Notification Service

Production-style Spring Boot notification platform demonstrating event-driven architecture, Transactional Outbox, Kafka, idempotent consumers, retries, Dead Letter Topics (DLT), event versioning, correlation IDs, observability, Docker Compose, Prometheus, Grafana, Alertmanager, and Docker Secrets.

## Overview

This project implements a reliable event-driven notification workflow:

```text
Client
  |
  | POST /api/users
  v
Spring Boot REST API
  |
  | @Transactional
  +------------------------------+
  |                              |
  v                              v
users table                outbox_events table
                                  |
                                  | Outbox Publisher
                                  v
                             Kafka topic
                        notifications.events
                           /              \\
                          /                \\
                         v                  v
               email-service         audit-service
                     |
                     | idempotency
                     v
              processed_events
```

Failure handling:

```text
Kafka Consumer
      |
      +--> Retryable failure
      |       |
      |       +--> retry
      |       +--> retry
      |       +--> DLT
      |
      +--> Unsupported event version
              |
              +--> DLT
```

Observability:

```text
Spring Boot
    |
    | Micrometer
    v
/actuator/prometheus
    |
    v
Prometheus
    |
    +--> Grafana
    |
    +--> Alertmanager
```

## Key Features

- Spring Boot REST API for user registration
- Transactional Outbox pattern
- PostgreSQL persistence
- Apache Kafka event backbone
- Kafka consumer groups for email and audit processing
- Idempotent event processing
- Duplicate-event protection using `event_id + consumer_name`
- Retry handling for retryable consumer failures
- Dead Letter Topic (DLT)
- Event schema/version validation
- Unsupported-version routing to DLT
- Correlation ID propagation
- Structured logging with MDC
- Micrometer application and business metrics
- Spring Boot Actuator health/readiness/metrics endpoints
- Prometheus scraping
- Grafana dashboard
- Prometheus alert rules
- Alertmanager routing
- Docker Compose development environment
- Docker Secrets for database password
- Testcontainers integration tests
- Graceful shutdown configuration
- Kafka partition/key strategy
- Configurable consumer concurrency

## Technology Stack

| Area | Technology |
|---|---|
| Language | Java 21 |
| Framework | Spring Boot |
| Web | Spring MVC / REST |
| Persistence | Spring Data JPA |
| Database | PostgreSQL 17 |
| Messaging | Apache Kafka 4.2.1 |
| Serialization | Jackson JSON |
| Build | Maven |
| Testing | JUnit 5, Awaitility, Testcontainers |
| Metrics | Micrometer |
| Monitoring | Prometheus |
| Dashboards | Grafana |
| Alerting | Alertmanager |
| Containerization | Docker / Docker Compose |

## Project Architecture

### API and Transactional Outbox

The registration operation is transactional:

```java
@Transactional
public void register(UserEventRequest request) {
    // user + outbox event in one database transaction
}
```

The application creates a unique event ID for each new business event:

```java
String eventId = UUID.randomUUID().toString();
```

This is intentional. `eventId` identifies one event instance. Idempotency is validated when the **same event is delivered more than once**, not when two independent registrations create two different events.

## Event Model

The main event is `UserRegisteredEvent`.

Conceptually:

```json
{
  "eventId": "uuid",
  "eventType": "user.registered",
  "eventVersion": 1,
  "occurredAt": "2026-01-01T10:00:00Z",
  "aggregateId": "user-123",
  "payload": {
    "name": "Rahul",
    "email": "user@example.com",
    "phone": "+919999999999"
  }
}
```

### Event identity vs Kafka key

- `eventId` = unique event identity
- `aggregateId` / `userId` = Kafka partitioning key

Publishing with the user ID as the Kafka key keeps events for the same user on the same partition, preserving ordering for that aggregate.

## Kafka Topics

Main topic:

```text
notifications.events
```

Dead Letter Topic:

```text
notifications.events.DLT
```

Consumer groups:

```text
email-service
audit-service
```

Each consumer group receives its own copy of the event stream.

## Idempotency

The consumer checks whether an event has already been processed:

```java
idempotencyService.alreadyProcessed(
    event.eventId(),
    "email-service"
)
```

If the event was already processed, business processing is skipped.

The database uses a unique constraint:

```text
(event_id, consumer_name)
```

This allows the same event to be processed independently by different consumers:

```text
event A + email-service  -> allowed
event A + email-service  -> duplicate
event A + audit-service  -> allowed
```

### Idempotency test

The integration test deliberately publishes the **same event object twice**:

```text
eventId = A

Kafka:
  A
  A
```

Expected:

```text
processed_events = 1
```

This is different from calling the registration API twice, because each new registration creates a new event ID.

## Retry and DLT

Retryable failures are handled separately from non-retryable failures.

Example:

```text
EmailDeliveryException
    -> retry
    -> retry
    -> DLT
```

Unsupported event versions are treated as non-retryable:

```text
eventVersion = 999
    -> UnsupportedEventVersionException
    -> DLT
```

## Event Versioning

The service validates `eventType` and `eventVersion`.

Supported:

```text
USER_REGISTERED V1
```

Unsupported versions are routed to:

```text
notifications.events.DLT
```

## Correlation ID and Observability

Correlation IDs flow through:

```text
HTTP request
    |
    v
MDC
    |
    v
Outbox event
    |
    v
Kafka header
    |
    v
Kafka consumer
    |
    v
consumer logs
```

Important log fields include `eventId`, `eventType`, `eventVersion`, `aggregateId/userId`, `correlationId`, and `retryCount` where applicable.

## Database Model

Core tables:

```text
users
outbox_events
processed_events
```

### `users`

Stores registered users.

### `outbox_events`

Stores events created as part of the business transaction.

Typical lifecycle:

```text
NEW
 |
 v
PROCESSING
 |
 +--> PUBLISHED
 |
 +--> retry
```

Stale `PROCESSING` records are recoverable.

### `processed_events`

Stores successfully processed consumer events. The uniqueness boundary is:

```text
event_id + consumer_name
```

## Transactional Outbox Flow

```text
POST /api/users
       |
       v
@Transactional
       |
       +--> users INSERT
       |
       +--> outbox_events INSERT
       |
       v
COMMIT
       |
       v
Outbox Publisher
       |
       v
Kafka
```

The database state and the intent to publish the event are committed together.

## Testing

Integration tests use Testcontainers for disposable PostgreSQL and Kafka instances.

Covered scenarios include:

- PostgreSQL integration
- Kafka integration
- Spring Boot integration context
- Supported V1 event
- Unsupported event version
- Duplicate event / idempotency
- Retryable failure
- DLT routing
- Observability-related integration behavior

Run all tests:

```bash
mvn clean test
```

Run one integration test:

```bash
mvn clean -Dtest=IdempotencyIntegrationTest test
```

## Docker Compose

The local deployment includes:

```text
PostgreSQL
Kafka
Spring Boot
Prometheus
Grafana
Alertmanager
```

Start:

```bash
docker compose up -d
```

Check:

```bash
docker compose ps
```

Stop:

```bash
docker compose down
```

Clean reset:

```bash
docker compose down -v
```

The Spring Boot container connects internally to:

```text
postgres:5432
kafka:9092
```

## Docker Secrets and Configuration

Database credentials are externalized and the PostgreSQL password is mounted through a Docker Secret rather than kept in Compose environment variables.

The repository should contain an `.env.example` template, but not the real `.env` or secret files.

Recommended Git ignore rules:

```gitignore
.env
.env.*
!.env.example
secrets/
```

## Observability

### Actuator

Health endpoints:

```text
/actuator/health
/actuator/health/liveness
/actuator/health/readiness
```

Metrics:

```text
/actuator/metrics
/actuator/prometheus
```

### Prometheus

Prometheus scrapes the Spring Boot Prometheus endpoint and exposes alert evaluation.

Open:

```text
http://localhost:9090
```

Check:

```text
Status -> Targets
```

The notification service target should be `UP`.

### Grafana

Grafana dashboards cover:

- Service health
- Notification processing throughput
- Outbox publish throughput
- Notification failures
- Duplicate event rate
- Unsupported event versions
- Outbox failures
- Outbox retries
- Stale recovery
- JVM CPU usage
- JVM heap utilization
- Kafka-related operational metrics

Open:

```text
http://localhost:3000
```

Inside Docker, the Prometheus datasource is:

```text
http://prometheus:9090
```

### Alertmanager

Prometheus alert rules cover conditions such as:

```text
NotificationServiceDown
NotificationProcessingFailures
OutboxPublishingFailures
OutboxRetriesIncreasing
UnsupportedEventVersions
HighDuplicateEventRate
JVMHeapUsageHigh
JVMHeapUsageCritical
```

Alertmanager handles grouping, severity-based routing, repeat intervals, and resolved notifications.

Open:

```text
http://localhost:9093
```

## Kafka Hardening

- Multiple partitions are configured for the main event topic.
- Kafka key is the user/aggregate ID.
- Consumer concurrency is configurable.
- Consumer groups are independent for email and audit processing.
- Graceful shutdown is configured.
- Local Docker Kafka uses development-friendly listener configuration.
- Kafka security properties are environment-driven so SASL/SSL can be enabled in a stronger deployment.

## Security

Implemented hardening includes:

- Database credentials externalized
- Docker Secret for the database password
- Actuator endpoint protection
- Management endpoint isolation
- Kafka security configuration designed to be environment-driven
- Development secrets excluded from Git
- No production credentials committed to source control
- UTC timezone for consistent distributed timestamps

## Running Locally

### Prerequisites

- JDK 21
- Maven
- Docker Desktop
- Git

### Configure

Create `.env` from `.env.example` and provide local values. Keep the real credentials out of Git.

### Run tests

```bash
mvn clean test
```

### Package

```bash
mvn clean package
```

### Start Docker environment

```bash
docker compose up -d
```

### Verify

```text
http://localhost:8081/actuator/health
http://localhost:9090
http://localhost:3000
http://localhost:9093
```

## Example API

### Register User

```http
POST /api/users
Content-Type: application/json
```

Example:

```json
{
  "userId": "user-001",
  "name": "Rahul",
  "email": "rahul@example.com",
  "phone": "+919999999999"
}
```

Expected workflow:

```text
REST
  ↓
users
  ↓
outbox_events
  ↓
Kafka
  ↓
email-service
  ↓
audit-service
```

## Failure Scenarios Tested

### Duplicate event

Same event ID published twice:

```text
A
A
```

Result:

```text
processed once
duplicate skipped
```

### Unsupported version

```text
eventVersion = 999
```

Result:

```text
DLT
```

### Retryable notification failure

A retryable notification failure is generated and handled through the configured retry/DLT path.

### Outbox failure

Outbox publishing failures update retry state and expose metrics.

### Stale processing recovery

Stale Outbox records in `PROCESSING` state can be recovered.

## Project Structure

```text
src/
├── main/
│   ├── java/com/rahul/notification/
│   │   ├── config/
│   │   ├── consumer/
│   │   ├── controller/
│   │   ├── entity/
│   │   ├── event/
│   │   ├── exception/
│   │   ├── observability/
│   │   ├── repository/
│   │   └── service/
│   │
│   └── resources/
│       └── application.yaml
│
└── test/
    └── java/com/rahul/notification/integration/

Dockerfile
docker-compose.yml
prometheus/
grafana/
alertmanager/
.env.example
.gitignore
pom.xml
README.md
```

## Design Decisions

### Why Outbox?

It avoids the classic dual-write problem where a database transaction succeeds but Kafka publishing fails.

### Why event ID?

It uniquely identifies one business event instance.

### Why Kafka key = user ID?

It provides partition affinity and ordering for events belonging to the same aggregate.

### Why consumer idempotency?

Kafka delivery can result in duplicate delivery, so business side effects must be protected at the consumer boundary.

### Why DLT?

A poison message should not block a partition indefinitely.

### Why event versioning?

It makes schema evolution explicit and prevents incompatible event versions from silently entering business logic.

## End-to-End Flow

### Successful flow

```text
POST /api/users
      ↓
User + Outbox transaction
      ↓
Outbox Publisher
      ↓
Kafka
      ↓
Email Consumer
      ↓
Idempotency Check
      ↓
Send Notification
      ↓
processed_events
```

### Failure flow

```text
Kafka
  ↓
Consumer
  ↓
Retryable exception
  ↓
Retry
  ↓
Retry
  ↓
DLT
```

### Observability flow

```text
Application
  ↓
Micrometer
  ↓
Prometheus
  ↓
Grafana
```

### Alert flow

```text
Prometheus
  ↓
Alert Rules
  ↓
Alertmanager
  ↓
Severity-based notification
```

## Current Scope

The current implementation focuses on:

- Event-driven architecture
- Transactional consistency
- Reliable event publication
- Idempotent consumers
- Retry and DLT
- Versioned events
- Correlation IDs and observability
- Dockerized local deployment
- Integration testing with Testcontainers
- Security hardening

Advanced production performance/load testing, full CI/CD, and Kubernetes deployment were intentionally skipped in the current roadmap and can be added later.

## Interview / Architecture Talking Points

This project demonstrates practical understanding of:

1. Transactional Outbox
2. At-least-once Kafka delivery
3. Consumer idempotency
4. Kafka consumer groups
5. Partition/key-based ordering
6. Retry and DLT strategies
7. Event schema versioning
8. Correlation ID propagation
9. Structured logging
10. Micrometer and Prometheus
11. Grafana dashboards
12. Alertmanager routing
13. Docker Compose networking
14. Testcontainers integration testing
15. Docker Secrets and configuration externalization
16. Graceful shutdown and readiness

## Author

**Rahul Moundekar**

Java / Spring Boot / Kafka backend portfolio project.
