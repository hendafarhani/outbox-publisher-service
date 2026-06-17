package com.microgo.outbox_publisher.service;

import com.microgo.outbox_publisher.entity.EventOutboxEntity;

public interface OutboxEventPublisher {

    void publish(EventOutboxEntity event) throws Exception;
}
