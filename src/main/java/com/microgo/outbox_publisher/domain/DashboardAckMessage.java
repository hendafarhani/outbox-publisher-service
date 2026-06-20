package com.microgo.outbox_publisher.domain;

import java.time.OffsetDateTime;

public record DashboardAckMessage(
        Long eventId,
        String status,
        OffsetDateTime processedAt,
        String service
) {
}
