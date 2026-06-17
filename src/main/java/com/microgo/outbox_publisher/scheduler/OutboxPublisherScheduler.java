package com.microgo.outbox_publisher.scheduler;

import com.microgo.outbox_publisher.entity.EventOutboxEntity;
import com.microgo.outbox_publisher.service.OutboxEventPublisher;
import com.microgo.outbox_publisher.service.OutboxStateManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnExpression("'${kafka.enabled:true}' == 'true' && '${outbox.publisher.enabled:true}' == 'true'")
public class OutboxPublisherScheduler {

    private final OutboxStateManager outboxStateManager;
    private final OutboxEventPublisher outboxEventPublisher;

    @Scheduled(fixedDelayString = "${outbox.publisher.fixed-delay-ms:1000}")
    public void publishPendingEvents() {
        outboxStateManager.claimPendingEvents().forEach(this::publishEvent);
    }

    private void publishEvent(EventOutboxEntity event) {
        try {
            publishToKafka(event);
            markAsPublished(event);
        } catch (Exception ex) {
            recordPublishFailure(event, ex);
        }
    }

    private void publishToKafka(EventOutboxEntity event) throws Exception {
        outboxEventPublisher.publish(event);
    }

    private void markAsPublished(EventOutboxEntity event) {
        outboxStateManager.markPublished(event.getId());
        log.debug("Published outbox event {} to Kafka", event.getId());
    }

    private void recordPublishFailure(EventOutboxEntity event, Exception ex) {
        outboxStateManager.recordPublishFailure(event.getId(), ex.getMessage());
        log.warn("Failed to publish outbox event {}", event.getId(), ex);
    }
}
