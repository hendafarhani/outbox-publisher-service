package com.microgo.outbox_publisher.enums;

public enum OutboxEventStatus {
    PENDING,
    PROCESSING,
    PUBLISHED,
    PROCESSED
}
