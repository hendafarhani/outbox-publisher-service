package com.microgo.outbox_publisher.service;

import com.microgo.outbox_publisher.entity.EventOutboxEntity;
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

    private final OutboxStateService outboxStateService;
    private final OutboxKafkaPublisher outboxKafkaPublisher;

    @Scheduled(fixedDelayString = "${outbox.publisher.fixed-delay-ms:1000}")
    public void publishPendingEvents() {
        for (EventOutboxEntity event : outboxStateService.claimPendingEvents()) {
            try {
                outboxKafkaPublisher.publish(event);
                outboxStateService.markPublished(event.getId());
                log.debug("Published outbox event {} to Kafka", event.getId());
            } catch (Exception ex) {
                outboxStateService.recordPublishFailure(event.getId(), ex.getMessage());
                log.warn("Failed to publish outbox event {}", event.getId(), ex);
            }
        }
    }
}
