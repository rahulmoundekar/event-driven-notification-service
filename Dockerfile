# ---------- Build stage ----------
FROM maven:3.9.11-eclipse-temurin-21 AS builder

WORKDIR /app

COPY pom.xml .

RUN mvn -B dependency:go-offline

COPY src ./src

RUN mvn -B clean package -DskipTests


# ---------- Runtime stage ----------
FROM eclipse-temurin:21-jre

WORKDIR /app

COPY --from=builder \
     /app/target/event-driven-notification-service-0.0.1-SNAPSHOT.jar \
     app.jar

EXPOSE 8081

ENTRYPOINT ["java", "-jar", "app.jar"]