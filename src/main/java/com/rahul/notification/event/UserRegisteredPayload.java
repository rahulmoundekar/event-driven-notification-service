package com.rahul.notification.event;

public record UserRegisteredPayload(
        String name,
        String email,
        String phone
) {
}