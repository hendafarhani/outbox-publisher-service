package com.microgo.outbox_publisher.model;

import java.time.OffsetDateTime;

public record DashboardAckMessage(
        Long eventId,
        String status,
        OffsetDateTime processedAt,
        String service
) {
}
