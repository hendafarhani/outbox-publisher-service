package com.microgo.outbox_publisher.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.microgo.outbox_publisher.entity.EventOutboxEntity;
import com.microgo.outbox_publisher.enums.OutboxEventStatus;
import com.microgo.outbox_publisher.model.OutboxEventEnvelope;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxEventEnvelopeBuilderTest {

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final OutboxEventEnvelopeBuilder builder = new OutboxEventEnvelopeBuilder(objectMapper);

    @Test
    void buildsEnvelopeWithDeliveryMetadataAndNestedPayload() throws Exception {
        OffsetDateTime createdAt = OffsetDateTime.parse("2026-06-13T10:00:00Z");
        EventOutboxEntity event = EventOutboxEntity.builder()
                .id(123L)
                .eventType("RIDER_NOTIFIED")
                .status(OutboxEventStatus.PROCESSING)
                .rideRequestId(42L)
                .rideRequestIdentifier("ride-abc")
                .requesterId("user-abc")
                .riderId("rider-123")
                .payload("""
                        {"eventType":"RIDER_NOTIFIED","rideStatus":"PENDING","rideRequestId":42}
                        """)
                .createdAt(createdAt)
                .updatedAt(createdAt)
                .build();

        OutboxEventEnvelope envelope = builder.build(event);
        String envelopeJson = builder.toJson(envelope);

        assertThat(envelope.eventId()).isEqualTo(123L);
        assertThat(envelope.eventType()).isEqualTo("RIDER_NOTIFIED");
        assertThat(envelope.eventTimestamp()).isEqualTo(createdAt);
        assertThat(envelope.rideRequestIdentifier()).isEqualTo("ride-abc");
        assertThat(envelope.rideStatus()).isEqualTo("PENDING");
        assertThat(objectMapper.readTree(envelopeJson).get("payload").get("rideRequestId").asLong()).isEqualTo(42L);
    }
}
