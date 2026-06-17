package com.microgo.outbox_publisher.service.serviceimpl;

import com.microgo.outbox_publisher.configuration.OutboxPublisherProperties;
import com.microgo.outbox_publisher.entity.EventOutboxEntity;
import com.microgo.outbox_publisher.enums.OutboxEventStatus;
import com.microgo.outbox_publisher.repository.EventOutboxRepository;
import com.microgo.outbox_publisher.service.OutboxStateManager;
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
public class OutboxStateManagerImpl implements OutboxStateManager {

    private final EventOutboxRepository eventOutboxRepository;
    private final OutboxPublisherProperties properties;

    @Override
    @Transactional
    public List<EventOutboxEntity> claimPendingEvents() {
        if (batchSizeIsDisabled()) {
            return List.of();
        }

        List<Long> eventIds = claimPendingEventIds();
        if (eventIds.isEmpty()) {
            return List.of();
        }

        List<EventOutboxEntity> events = loadEventsInClaimOrder(eventIds);
        markEventsAsProcessing(events);
        return eventOutboxRepository.saveAll(events);
    }

    @Override
    @Transactional
    public EventOutboxEntity markPublished(Long eventId) {
        EventOutboxEntity event = loadEvent(eventId);
        if (!isProcessing(event)) {
            return event;
        }
        markEventAsPublished(event);
        return eventOutboxRepository.save(event);
    }

    @Override
    @Transactional
    public EventOutboxEntity recordPublishFailure(Long eventId, String errorMessage) {
        EventOutboxEntity event = loadEvent(eventId);
        markEventAsPendingForRetry(event, errorMessage);
        return eventOutboxRepository.save(event);
    }

    @Override
    @Transactional
    public EventOutboxEntity markProcessed(Long eventId) {
        EventOutboxEntity event = loadEvent(eventId);
        if (isProcessed(event)) {
            return event;
        }
        ensurePublishedBeforeProcessing(event);
        markEventAsProcessed(event);
        return eventOutboxRepository.save(event);
    }

    @Override
    @Transactional
    public List<EventOutboxEntity> resetStalePublishedEvents() {
        List<EventOutboxEntity> retryableEvents = findRetryableStalePublishedEvents();
        markEventsAsTimedOut(retryableEvents);
        return eventOutboxRepository.saveAll(retryableEvents);
    }

    private boolean batchSizeIsDisabled() {
        return properties.batchSize() <= 0;
    }

    private List<Long> claimPendingEventIds() {
        return eventOutboxRepository.claimPendingEventIds(properties.batchSize());
    }

    private List<EventOutboxEntity> loadEventsInClaimOrder(List<Long> eventIds) {
        Map<Long, EventOutboxEntity> eventsById = eventOutboxRepository.findAllById(eventIds).stream()
                .collect(Collectors.toMap(EventOutboxEntity::getId, Function.identity()));
        return eventIds.stream()
                .map(eventsById::get)
                .toList();
    }

    private void markEventsAsProcessing(List<EventOutboxEntity> events) {
        OffsetDateTime now = OffsetDateTime.now();
        events.forEach(event -> markEventAsProcessing(event, now));
    }

    private void markEventAsProcessing(EventOutboxEntity event, OffsetDateTime now) {
        event.setStatus(OutboxEventStatus.PROCESSING);
        event.setUpdatedAt(now);
        event.setLastError(null);
    }

    private boolean isProcessing(EventOutboxEntity event) {
        return event.getStatus() == OutboxEventStatus.PROCESSING;
    }

    private void markEventAsPublished(EventOutboxEntity event) {
        event.setStatus(OutboxEventStatus.PUBLISHED);
        event.setUpdatedAt(OffsetDateTime.now());
        event.setLastError(null);
    }

    private void markEventAsPendingForRetry(EventOutboxEntity event, String errorMessage) {
        event.setStatus(OutboxEventStatus.PENDING);
        incrementRetryCount(event);
        event.setLastError(errorMessage);
        event.setUpdatedAt(OffsetDateTime.now());
    }

    private boolean isProcessed(EventOutboxEntity event) {
        return event.getStatus() == OutboxEventStatus.PROCESSED;
    }

    private void ensurePublishedBeforeProcessing(EventOutboxEntity event) {
        if (event.getStatus() != OutboxEventStatus.PUBLISHED) {
            throw new IllegalStateException("Cannot process outbox event " + event.getId() + " while status is " + event.getStatus());
        }
    }

    private void markEventAsProcessed(EventOutboxEntity event) {
        OffsetDateTime now = OffsetDateTime.now();
        event.setStatus(OutboxEventStatus.PROCESSED);
        event.setProcessedAt(now);
        event.setUpdatedAt(now);
        event.setLastError(null);
    }

    private List<EventOutboxEntity> findRetryableStalePublishedEvents() {
        return findStalePublishedEvents().stream()
                .filter(this::canRetry)
                .toList();
    }

    private List<EventOutboxEntity> findStalePublishedEvents() {
        return eventOutboxRepository.findByStatusAndUpdatedAtBeforeOrderByUpdatedAtAsc(
                OutboxEventStatus.PUBLISHED,
                ackTimeoutThreshold()
        );
    }

    private OffsetDateTime ackTimeoutThreshold() {
        return OffsetDateTime.now().minusSeconds(properties.ackTimeoutSeconds());
    }

    private boolean canRetry(EventOutboxEntity event) {
        return event.getRetryCount() < properties.maxRetryCount();
    }

    private void markEventsAsTimedOut(List<EventOutboxEntity> events) {
        OffsetDateTime now = OffsetDateTime.now();
        events.forEach(event -> markEventAsTimedOut(event, now));
    }

    private void markEventAsTimedOut(EventOutboxEntity event, OffsetDateTime now) {
        event.setStatus(OutboxEventStatus.PENDING);
        incrementRetryCount(event);
        event.setLastError("Dashboard acknowledgement timed out");
        event.setUpdatedAt(now);
    }

    private void incrementRetryCount(EventOutboxEntity event) {
        event.setRetryCount(event.getRetryCount() + 1);
    }

    private EventOutboxEntity loadEvent(Long eventId) {
        return eventOutboxRepository.findById(eventId)
                .orElseThrow(() -> new EntityNotFoundException("Outbox event not found for id " + eventId));
    }
}
