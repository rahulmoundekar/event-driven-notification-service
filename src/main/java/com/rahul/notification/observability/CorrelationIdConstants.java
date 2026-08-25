package com.rahul.notification.observability;

public final class CorrelationIdConstants {

    private CorrelationIdConstants() {
    }

    public static final String HEADER_NAME =
            "X-Correlation-Id";

    public static final String MDC_KEY =
            "correlationId";

    public static final String KAFKA_HEADER =
            "correlationId";
}