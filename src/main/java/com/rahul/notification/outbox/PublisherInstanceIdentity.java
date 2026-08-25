package com.rahul.notification.outbox;

import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class PublisherInstanceIdentity {

    private final String instanceId =
            UUID.randomUUID().toString();

    public String getInstanceId() {
        return instanceId;
    }
}