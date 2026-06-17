package com.microgo.outbox_publisher.repository;

import com.microgo.outbox_publisher.entity.EventOutboxEntity;
import com.microgo.outbox_publisher.enums.OutboxEventStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.OffsetDateTime;
import java.util.List;

public interface EventOutboxRepository extends JpaRepository<EventOutboxEntity, Long>, EventOutboxClaimRepository {

    List<EventOutboxEntity> findByStatusAndUpdatedAtBeforeOrderByUpdatedAtAsc(OutboxEventStatus status, OffsetDateTime updatedBefore);
}
