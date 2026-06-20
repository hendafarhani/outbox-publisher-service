package com.microgo.outbox_publisher.service.serviceimpl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microgo.outbox_publisher.entity.EventOutboxEntity;
import com.microgo.outbox_publisher.domain.OutboxEventEnvelope;
import com.microgo.outbox_publisher.service.OutboxEventEnvelopeFactory;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OutboxEventEnvelopeFactoryImpl implements OutboxEventEnvelopeFactory {

    private final ObjectMapper objectMapper;

    @Override
    public OutboxEventEnvelope build(EventOutboxEntity event) {
        JsonNode payload = readPayload(event);
        return buildEnvelope(event, payload);
    }

    private OutboxEventEnvelope buildEnvelope(EventOutboxEntity event, JsonNode payload) {
        return new OutboxEventEnvelope(
                event.getId(),
                event.getEventType(),
                event.getCreatedAt(),
                event.getRideRequestIdentifier(),
                event.getRequesterId(),
                event.getRiderId(),
                rideStatusFrom(payload),
                payload
        );
    }

    @Override
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

    private String rideStatusFrom(JsonNode payload) {
        JsonNode rideStatus = payload.path("rideStatus");
        return rideStatus.isMissingNode() ? null : rideStatus.asText(null);
    }
}
