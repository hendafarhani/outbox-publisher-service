package com.microgo.outbox_publisher.service;

import com.microgo.outbox_publisher.entity.EventOutboxEntity;
import com.microgo.outbox_publisher.model.OutboxEventEnvelope;

public interface OutboxEventEnvelopeFactory {

    OutboxEventEnvelope build(EventOutboxEntity event);

    String toJson(OutboxEventEnvelope envelope);
}
