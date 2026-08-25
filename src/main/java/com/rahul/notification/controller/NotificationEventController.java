package com.rahul.notification.controller;

import com.rahul.notification.dto.UserEventRequest;
import com.rahul.notification.event.UserRegisteredEvent;
import com.rahul.notification.event.UserRegisteredPayload;
import com.rahul.notification.producer.NotificationEventProducer;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/api/events")
@RequiredArgsConstructor
public class NotificationEventController {

    private final NotificationEventProducer producer;

    @PostMapping("/user-registered")
    public ResponseEntity<String> publishUserRegistered(
            @Valid @RequestBody UserEventRequest request) {

        UserRegisteredPayload payload =
                new UserRegisteredPayload(
                        request.name(),
                        request.email(),
                        request.phone()
                );

        UserRegisteredEvent event =
                new UserRegisteredEvent(
                        UUID.randomUUID().toString(),
                        "USER_REGISTERED",
                        1,
                        Instant.now(),
                        request.userId(),
                        payload
                );

        producer.publishUserRegisteredEvent(event);

        return ResponseEntity.accepted()
                .body("UserRegistered event published");
    }
}