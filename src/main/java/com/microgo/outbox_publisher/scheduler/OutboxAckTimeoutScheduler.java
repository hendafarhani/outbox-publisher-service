package com.microgo.outbox_publisher.scheduler;

import com.microgo.outbox_publisher.service.OutboxStateManager;
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

    private final OutboxStateManager outboxStateManager;

    @Scheduled(fixedDelayString = "${outbox.publisher.fixed-delay-ms:1000}")
    public void resetStalePublishedEvents() {
        int resetCount = resetTimedOutEvents();
        logResetCount(resetCount);
    }

    private int resetTimedOutEvents() {
        return outboxStateManager.resetStalePublishedEvents().size();
    }

    private void logResetCount(int resetCount) {
        if (resetCount > 0) {
            log.warn("Reset {} stale published outbox events to pending", resetCount);
        }
    }
}
