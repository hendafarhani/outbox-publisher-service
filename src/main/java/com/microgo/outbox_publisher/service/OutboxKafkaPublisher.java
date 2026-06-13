package com.microgo.outbox_publisher.service;

import com.microgo.outbox_publisher.configuration.OutboxPublisherProperties;
import com.microgo.outbox_publisher.entity.EventOutboxEntity;
import com.microgo.outbox_publisher.model.OutboxEventEnvelope;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "kafka.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxKafkaPublisher {

    private static final int SEND_TIMEOUT_SECONDS = 10;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxPublisherProperties properties;
    private final OutboxEventEnvelopeBuilder envelopeBuilder;

    public void publish(EventOutboxEntity event) throws Exception {
        OutboxEventEnvelope envelope = envelopeBuilder.build(event);
        ProducerRecord<String, String> record = new ProducerRecord<>(
                properties.eventTopic(),
                event.getRideRequestIdentifier(),
                envelopeBuilder.toJson(envelope)
        );
        addHeader(record, "eventId", String.valueOf(event.getId()));
        addHeader(record, "eventType", event.getEventType());
        addHeader(record, "rideRequestIdentifier", event.getRideRequestIdentifier());

        kafkaTemplate.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private void addHeader(ProducerRecord<String, String> record, String name, String value) {
        if (value != null) {
            record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
