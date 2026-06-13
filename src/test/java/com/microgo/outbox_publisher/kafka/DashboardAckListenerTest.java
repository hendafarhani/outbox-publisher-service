package com.microgo.outbox_publisher.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.microgo.outbox_publisher.service.OutboxStateService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class DashboardAckListenerTest {

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final OutboxStateService outboxStateService = mock(OutboxStateService.class);
    private final DashboardAckListener listener = new DashboardAckListener(objectMapper, outboxStateService);

    @Test
    void marksEventProcessedWhenDashboardPublishedToWebSocket() {
        listener.listen("""
                {"eventId":123,"status":"WEBSOCKET_PUBLISHED","processedAt":"2026-06-13T10:00:03Z","service":"dashboard-service"}
                """);

        verify(outboxStateService).markProcessed(123L);
    }

    @Test
    void ignoresMalformedAndNonPublishedAcknowledgements() {
        listener.listen("not-json");
        listener.listen("""
                {"eventId":123,"status":"FAILED","processedAt":"2026-06-13T10:00:03Z","service":"dashboard-service"}
                """);

        verify(outboxStateService, never()).markProcessed(123L);
    }
}
