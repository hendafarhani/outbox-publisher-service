package com.microgo.outbox_publisher.kafka.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microgo.outbox_publisher.domain.DashboardAckMessage;
import com.microgo.outbox_publisher.service.OutboxStateManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Slf4j
@Component
@RequiredArgsConstructor
public class DashboardAckHandler {

    private static final String WEBSOCKET_PUBLISHED = "WEBSOCKET_PUBLISHED";

    private final ObjectMapper objectMapper;
    private final OutboxStateManager outboxStateManager;

    @KafkaListener(
            id = "${outbox.publisher.ack-listener-id}",
            topics = "${outbox.publisher.ack-topic}",
            groupId = "${outbox.publisher.ack-group-id}",
            containerFactory = "outboxAckKafkaListenerContainerFactory"
    )
    public void listen(String message) {
        DashboardAckMessage ack = readAck(message);
        if (cannotProcessAck(ack, message)) {
            return;
        }
        markEventAsProcessed(ack);
    }

    private DashboardAckMessage readAck(String message) {
        if (!StringUtils.hasText(message)) {
            log.warn("Ignoring blank dashboard acknowledgement");
            return null;
        }
        try {
            return objectMapper.readValue(message, DashboardAckMessage.class);
        } catch (JsonProcessingException ex) {
            log.warn("Ignoring malformed dashboard acknowledgement {}", message, ex);
            return null;
        }
    }

    private boolean cannotProcessAck(DashboardAckMessage ack, String message) {
        if (ack == null) {
            return true;
        }
        if (isInvalidPublishedAck(ack)) {
            log.warn("Ignoring invalid dashboard acknowledgement {}", message);
            return true;
        }
        return false;
    }

    private boolean isInvalidPublishedAck(DashboardAckMessage ack) {
        return ack.eventId() == null || !isWebsocketPublished(ack);
    }

    private boolean isWebsocketPublished(DashboardAckMessage ack) {
        return WEBSOCKET_PUBLISHED.equals(ack.status());
    }

    private void markEventAsProcessed(DashboardAckMessage ack) {
        outboxStateManager.markProcessed(ack.eventId());
        log.debug("Marked outbox event {} as processed from dashboard acknowledgement", ack.eventId());
    }
}
