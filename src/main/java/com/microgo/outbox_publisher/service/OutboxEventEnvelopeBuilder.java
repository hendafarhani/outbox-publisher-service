package com.microgo.outbox_publisher.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microgo.outbox_publisher.entity.EventOutboxEntity;
import com.microgo.outbox_publisher.model.OutboxEventEnvelope;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OutboxEventEnvelopeBuilder {

    private final ObjectMapper objectMapper;

    public OutboxEventEnvelope build(EventOutboxEntity event) {
        JsonNode payload = readPayload(event);
        return new OutboxEventEnvelope(
                event.getId(),
                event.getEventType(),
                event.getCreatedAt(),
                event.getRideRequestIdentifier(),
                event.getRequesterId(),
                event.getRiderId(),
                payload.path("rideStatus").isMissingNode() ? null : payload.path("rideStatus").asText(null),
                payload
        );
    }

    public String toJson(OutboxEventEnvelope envelope) {
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Unable to serialize outbox event envelope " + envelope.eventId(), ex);
        }
    }

    private JsonNode readPayload(EventOutboxEntity event) {
        try {
            return objectMapper.readTree(event.getPayload());
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Unable to parse outbox payload " + event.getId(), ex);
        }
    }
}
