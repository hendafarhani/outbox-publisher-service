package com.microgo.outbox_publisher.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microgo.outbox_publisher.model.DashboardAckMessage;
import com.microgo.outbox_publisher.service.OutboxStateService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "kafka.enabled", havingValue = "true", matchIfMissing = true)
public class DashboardAckListener {

    private static final String WEBSOCKET_PUBLISHED = "WEBSOCKET_PUBLISHED";

    private final ObjectMapper objectMapper;
    private final OutboxStateService outboxStateService;

    @KafkaListener(
            id = "outboxAckListener",
            topics = "${outbox.publisher.ack-topic:ride.request.events.acks}",
            groupId = "${outbox.publisher.ack-group-id:outbox-publisher-service.group}",
            containerFactory = "outboxAckKafkaListenerContainerFactory"
    )
    public void listen(String message) {
        DashboardAckMessage ack = readAck(message);
        if (ack == null) {
            return;
        }
        if (ack.eventId() == null || !WEBSOCKET_PUBLISHED.equals(ack.status())) {
            log.warn("Ignoring invalid dashboard acknowledgement {}", message);
            return;
        }

        outboxStateService.markProcessed(ack.eventId());
        log.debug("Marked outbox event {} as processed from dashboard acknowledgement", ack.eventId());
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
}
