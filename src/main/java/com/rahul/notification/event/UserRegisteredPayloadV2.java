package com.rahul.notification.event;

public record UserRegisteredPayloadV2(
        String name,
        String email,
        String phone,
        String locale,
        boolean marketingOptIn
) {
}
