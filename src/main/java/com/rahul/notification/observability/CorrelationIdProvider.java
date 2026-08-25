package com.rahul.notification.observability;

import org.slf4j.MDC;
import org.springframework.stereotype.Component;

@Component
public class CorrelationIdProvider {

    public String getCurrentCorrelationId() {

        return MDC.get(
                CorrelationIdConstants.MDC_KEY
        );
    }
}