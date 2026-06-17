package com.microgo.outbox_publisher.repository;

import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
public class EventOutboxClaimRepositoryImpl implements EventOutboxClaimRepository {

    private final EntityManager entityManager;

    @Override
    public List<Long> claimPendingEventIds(int batchSize) {
        return queryPendingEventIds(batchSize).stream()
                .map(Number::longValue)
                .toList();
    }

    @SuppressWarnings("unchecked")
    private List<Number> queryPendingEventIds(int batchSize) {
        return entityManager.createNativeQuery("""
                        SELECT id
                        FROM event_outbox
                        WHERE status = 'PENDING'
                        ORDER BY created_at
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED
                        """)
                .setParameter("limit", batchSize)
                .getResultList();
    }
}
