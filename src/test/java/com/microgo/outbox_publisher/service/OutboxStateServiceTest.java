package com.microgo.outbox_publisher.service;

import com.microgo.outbox_publisher.configuration.OutboxPublisherProperties;
import com.microgo.outbox_publisher.entity.EventOutboxEntity;
import com.microgo.outbox_publisher.enums.OutboxEventStatus;
import com.microgo.outbox_publisher.repository.EventOutboxRepository;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxStateServiceTest {

    private final EventOutboxRepository repository = mock(EventOutboxRepository.class);
    private final OutboxStateService service = new OutboxStateService(
            repository,
            mock(EntityManager.class),
            new OutboxPublisherProperties(null, null, null, null, null, 50, null, 30L, 10)
    );

    @Test
    void markPublishedTransitionsOnlyProcessingEvents() {
        EventOutboxEntity event = event(OutboxEventStatus.PROCESSING);
        when(repository.findById(123L)).thenReturn(Optional.of(event));
        when(repository.save(event)).thenReturn(event);

        EventOutboxEntity result = service.markPublished(123L);

        assertThat(result.getStatus()).isEqualTo(OutboxEventStatus.PUBLISHED);
        assertThat(result.getLastError()).isNull();
        verify(repository).save(event);
    }

    @Test
    void recordPublishFailureReturnsEventToPendingAndIncrementsRetryCount() {
        EventOutboxEntity event = event(OutboxEventStatus.PROCESSING);
        event.setRetryCount(2);
        when(repository.findById(123L)).thenReturn(Optional.of(event));
        when(repository.save(event)).thenReturn(event);

        EventOutboxEntity result = service.recordPublishFailure(123L, "Kafka unavailable");

        assertThat(result.getStatus()).isEqualTo(OutboxEventStatus.PENDING);
        assertThat(result.getRetryCount()).isEqualTo(3);
        assertThat(result.getLastError()).isEqualTo("Kafka unavailable");
    }

    @Test
    void markProcessedTransitionsPublishedEventAndIsIdempotent() {
        EventOutboxEntity event = event(OutboxEventStatus.PUBLISHED);
        when(repository.findById(123L)).thenReturn(Optional.of(event));
        when(repository.save(event)).thenReturn(event);

        EventOutboxEntity result = service.markProcessed(123L);

        assertThat(result.getStatus()).isEqualTo(OutboxEventStatus.PROCESSED);
        assertThat(result.getProcessedAt()).isNotNull();

        when(repository.findById(123L)).thenReturn(Optional.of(result));
        assertThat(service.markProcessed(123L).getStatus()).isEqualTo(OutboxEventStatus.PROCESSED);
    }

    @Test
    void markProcessedRejectsPendingEvent() {
        when(repository.findById(123L)).thenReturn(Optional.of(event(OutboxEventStatus.PENDING)));

        assertThatThrownBy(() -> service.markProcessed(123L))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void resetStalePublishedEventsReturnsRetryableEventsToPending() {
        EventOutboxEntity stale = event(OutboxEventStatus.PUBLISHED);
        stale.setUpdatedAt(OffsetDateTime.now().minusSeconds(60));
        when(repository.findByStatusAndUpdatedAtBeforeOrderByUpdatedAtAsc(any(), any()))
                .thenReturn(List.of(stale));
        when(repository.saveAll(List.of(stale))).thenReturn(List.of(stale));

        List<EventOutboxEntity> result = service.resetStalePublishedEvents();

        assertThat(result).containsExactly(stale);
        assertThat(stale.getStatus()).isEqualTo(OutboxEventStatus.PENDING);
        assertThat(stale.getRetryCount()).isEqualTo(1);
        assertThat(stale.getLastError()).isEqualTo("Dashboard acknowledgement timed out");
    }

    private EventOutboxEntity event(OutboxEventStatus status) {
        OffsetDateTime now = OffsetDateTime.now();
        return EventOutboxEntity.builder()
                .id(123L)
                .eventType("RIDER_NOTIFIED")
                .status(status)
                .rideRequestId(42L)
                .rideRequestIdentifier("ride-abc")
                .requesterId("user-abc")
                .payload("{}")
                .createdAt(now)
                .updatedAt(now)
                .build();
    }
}
