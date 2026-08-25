package com.rahul.notification.controller;

import com.rahul.notification.dto.UserEventRequest;
import com.rahul.notification.service.UserRegistrationService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class UserRegistrationController {

    private final UserRegistrationService userRegistrationService;

    @PostMapping
    public ResponseEntity<String> registerUser(
            @Valid @RequestBody UserEventRequest request) {

        userRegistrationService.register(request);

        return ResponseEntity.ok(
                "User registered successfully"
        );
    }
}