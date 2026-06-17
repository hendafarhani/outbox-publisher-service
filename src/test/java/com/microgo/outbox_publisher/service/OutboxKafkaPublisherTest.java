package com.microgo.outbox_publisher.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.microgo.outbox_publisher.configuration.OutboxPublisherProperties;
import com.microgo.outbox_publisher.entity.EventOutboxEntity;
import com.microgo.outbox_publisher.enums.OutboxEventStatus;
import com.microgo.outbox_publisher.service.serviceimpl.OutboxEventEnvelopeFactoryImpl;
import com.microgo.outbox_publisher.service.serviceimpl.OutboxEventPublisherImpl;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxEventPublisherImplTest {

    @Test
    void sendsEnvelopeWithKeyAndHeaders() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        OutboxPublisherProperties properties = new OutboxPublisherProperties(
                "ride.request.events", "ride.request.events.acks", 3, 3, (short) 1, 50, 1000L, 30L, 10);
        OutboxEventPublisherImpl publisher = new OutboxEventPublisherImpl(
                kafkaTemplate,
                properties,
                new OutboxEventEnvelopeFactoryImpl(objectMapper)
        );
        ArgumentCaptor<ProducerRecord<String, String>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(mock(SendResult.class));
        when(kafkaTemplate.send(recordCaptor.capture())).thenReturn(future);

        publisher.publish(event());

        ProducerRecord<String, String> record = recordCaptor.getValue();
        assertThat(record.topic()).isEqualTo("ride.request.events");
        assertThat(record.key()).isEqualTo("ride-abc");
        assertThat(record.headers().lastHeader("eventId").value()).isEqualTo("123".getBytes(StandardCharsets.UTF_8));
        assertThat(record.headers().lastHeader("eventType").value()).isEqualTo("RIDER_NOTIFIED".getBytes(StandardCharsets.UTF_8));
        assertThat(record.headers().lastHeader("rideRequestIdentifier").value()).isEqualTo("ride-abc".getBytes(StandardCharsets.UTF_8));
        assertThat(objectMapper.readTree(record.value()).get("eventId").asLong()).isEqualTo(123L);
        assertThat(objectMapper.readTree(record.value()).get("payload").get("rideStatus").asText()).isEqualTo("PENDING");
        verify(kafkaTemplate).send(record);
    }

    private EventOutboxEntity event() {
        OffsetDateTime now = OffsetDateTime.parse("2026-06-13T10:00:00Z");
        return EventOutboxEntity.builder()
                .id(123L)
                .eventType("RIDER_NOTIFIED")
                .status(OutboxEventStatus.PROCESSING)
                .rideRequestId(42L)
                .rideRequestIdentifier("ride-abc")
                .requesterId("user-abc")
                .riderId("rider-123")
                .payload("{\"rideStatus\":\"PENDING\"}")
                .createdAt(now)
                .updatedAt(now)
                .build();
    }
}
