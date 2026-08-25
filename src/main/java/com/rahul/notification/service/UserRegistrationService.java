package com.rahul.notification.service;

import com.rahul.notification.dto.UserEventRequest;
import com.rahul.notification.entity.OutboxEvent;
import com.rahul.notification.entity.User;
import com.rahul.notification.event.EventTypes;
import com.rahul.notification.event.EventVersions;
import com.rahul.notification.event.UserRegisteredEvent;
import com.rahul.notification.event.UserRegisteredPayload;
import com.rahul.notification.observability.CorrelationIdProvider;
import com.rahul.notification.repository.OutboxEventRepository;
import com.rahul.notification.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class UserRegistrationService {

    private final UserRepository userRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final CorrelationIdProvider correlationIdProvider;

    @Transactional
    public void register(UserEventRequest request) throws JacksonException {

        String eventId = UUID.randomUUID().toString();

        User user = new User(request.userId(), request.name(), request.email(), request.phone());

        userRepository.save(user);

        UserRegisteredPayload payload = new UserRegisteredPayload(request.name(), request.email(), request.phone());

        UserRegisteredEvent event = new UserRegisteredEvent(eventId, EventTypes.USER_REGISTERED, EventVersions.USER_REGISTERED_V1, Instant.now(), request.userId(), payload);

        String correlationId = correlationIdProvider.getCurrentCorrelationId();

        String eventJson = objectMapper.writeValueAsString(event);

        OutboxEvent outboxEvent = new OutboxEvent(eventId, "USER_REGISTERED", "USER", request.userId(), eventJson, correlationId);

        outboxEventRepository.save(outboxEvent);
    }
}