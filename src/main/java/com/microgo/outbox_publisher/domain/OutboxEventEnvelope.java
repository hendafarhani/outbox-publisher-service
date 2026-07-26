package com.microgo.outbox_publisher.domain;

import tools.jackson.databind.JsonNode;

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
