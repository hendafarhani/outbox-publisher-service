package com.microgo.outbox_publisher.domain;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.OffsetDateTime;

public record OutboxEventEnvelope(
        Long eventId,
        String eventType,
        OffsetDateTime eventTimestamp,
        String rideRequestIdentifier,
        String requesterId,
        String riderId,
        String rideStatus,
        JsonNode payload
) {
}
