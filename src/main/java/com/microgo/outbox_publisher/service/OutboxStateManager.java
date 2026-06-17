package com.microgo.outbox_publisher.service;

import com.microgo.outbox_publisher.entity.EventOutboxEntity;

import java.util.List;

public interface OutboxStateManager {

    List<EventOutboxEntity> claimPendingEvents();

    EventOutboxEntity markPublished(Long eventId);

    EventOutboxEntity recordPublishFailure(Long eventId, String errorMessage);

    EventOutboxEntity markProcessed(Long eventId);

    List<EventOutboxEntity> resetStalePublishedEvents();
}
