package com.microgo.outbox_publisher.repository;

import java.util.List;

public interface EventOutboxClaimRepository {

    List<Long> claimPendingEventIds(int batchSize);
}
