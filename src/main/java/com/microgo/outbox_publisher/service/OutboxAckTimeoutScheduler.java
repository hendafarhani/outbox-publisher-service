package com.microgo.outbox_publisher.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "outbox.publisher.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxAckTimeoutScheduler {

    private final OutboxStateService outboxStateService;

    @Scheduled(fixedDelayString = "${outbox.publisher.fixed-delay-ms:1000}")
    public void resetStalePublishedEvents() {
        int resetCount = outboxStateService.resetStalePublishedEvents().size();
        if (resetCount > 0) {
            log.warn("Reset {} stale published outbox events to pending", resetCount);
        }
    }
}
