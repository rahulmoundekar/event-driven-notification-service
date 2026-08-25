package com.rahul.notification.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;

public record UserEventRequest(

        @NotBlank
        String userId,

        @NotBlank
        String name,

        @NotBlank
        @Email
        String email,

        @NotBlank
        String phone
) {
}