package com.microgo.outbox_publisher.service;

import com.microgo.outbox_publisher.configuration.OutboxPublisherProperties;
import com.microgo.outbox_publisher.entity.EventOutboxEntity;
import com.microgo.outbox_publisher.enums.OutboxEventStatus;
import com.microgo.outbox_publisher.repository.EventOutboxRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class OutboxStateService {

    private final EventOutboxRepository eventOutboxRepository;
    private final EntityManager entityManager;
    private final OutboxPublisherProperties properties;

    @Transactional
    public List<EventOutboxEntity> claimPendingEvents() {
        if (properties.batchSize() <= 0) {
            return List.of();
        }

        @SuppressWarnings("unchecked")
        List<Number> ids = entityManager.createNativeQuery("""
                        SELECT id
                        FROM event_outbox
                        WHERE status = 'PENDING'
                        ORDER BY created_at
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED
                        """)
                .setParameter("limit", properties.batchSize())
                .getResultList();

        if (ids.isEmpty()) {
            return List.of();
        }

        OffsetDateTime now = OffsetDateTime.now();
        List<Long> eventIds = ids.stream().map(Number::longValue).toList();
        Map<Long, EventOutboxEntity> eventsById = eventOutboxRepository.findAllById(eventIds).stream()
                .collect(Collectors.toMap(EventOutboxEntity::getId, Function.identity()));
        List<EventOutboxEntity> events = eventIds.stream()
                .map(eventsById::get)
                .toList();
        events.forEach(event -> {
            event.setStatus(OutboxEventStatus.PROCESSING);
            event.setUpdatedAt(now);
            event.setLastError(null);
        });
        return eventOutboxRepository.saveAll(events);
    }

    @Transactional
    public EventOutboxEntity markPublished(Long eventId) {
        EventOutboxEntity event = loadEvent(eventId);
        if (event.getStatus() != OutboxEventStatus.PROCESSING) {
            return event;
        }
        event.setStatus(OutboxEventStatus.PUBLISHED);
        event.setUpdatedAt(OffsetDateTime.now());
        event.setLastError(null);
        return eventOutboxRepository.save(event);
    }

    @Transactional
    public EventOutboxEntity recordPublishFailure(Long eventId, String errorMessage) {
        EventOutboxEntity event = loadEvent(eventId);
        event.setStatus(OutboxEventStatus.PENDING);
        event.setRetryCount(event.getRetryCount() + 1);
        event.setLastError(errorMessage);
        event.setUpdatedAt(OffsetDateTime.now());
        return eventOutboxRepository.save(event);
    }

    @Transactional
    public EventOutboxEntity markProcessed(Long eventId) {
        EventOutboxEntity event = loadEvent(eventId);
        if (event.getStatus() == OutboxEventStatus.PROCESSED) {
            return event;
        }
        if (event.getStatus() != OutboxEventStatus.PUBLISHED) {
            throw new IllegalStateException("Cannot process outbox event " + eventId + " while status is " + event.getStatus());
        }
        OffsetDateTime now = OffsetDateTime.now();
        event.setStatus(OutboxEventStatus.PROCESSED);
        event.setProcessedAt(now);
        event.setUpdatedAt(now);
        event.setLastError(null);
        return eventOutboxRepository.save(event);
    }

    @Transactional
    public List<EventOutboxEntity> resetStalePublishedEvents() {
        OffsetDateTime staleBefore = OffsetDateTime.now().minusSeconds(properties.ackTimeoutSeconds());
        List<EventOutboxEntity> staleEvents = eventOutboxRepository.findByStatusAndUpdatedAtBeforeOrderByUpdatedAtAsc(
                OutboxEventStatus.PUBLISHED,
                staleBefore
        );
        OffsetDateTime now = OffsetDateTime.now();
        List<EventOutboxEntity> retryableEvents = staleEvents.stream()
                .filter(event -> event.getRetryCount() < properties.maxRetryCount())
                .toList();
        retryableEvents.forEach(event -> {
                    event.setStatus(OutboxEventStatus.PENDING);
                    event.setRetryCount(event.getRetryCount() + 1);
                    event.setLastError("Dashboard acknowledgement timed out");
                    event.setUpdatedAt(now);
                });
        return eventOutboxRepository.saveAll(retryableEvents);
    }

    private EventOutboxEntity loadEvent(Long eventId) {
        return eventOutboxRepository.findById(eventId)
                .orElseThrow(() -> new EntityNotFoundException("Outbox event not found for id " + eventId));
    }
}
