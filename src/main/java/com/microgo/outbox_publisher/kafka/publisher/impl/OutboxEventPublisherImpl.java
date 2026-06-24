package com.microgo.outbox_publisher.kafka.publisher.impl;

import com.microgo.outbox_publisher.configuration.OutboxPublisherProperties;
import com.microgo.outbox_publisher.entity.EventOutboxEntity;
import com.microgo.outbox_publisher.domain.OutboxEventEnvelope;
import com.microgo.outbox_publisher.service.OutboxEventEnvelopeFactory;
import com.microgo.outbox_publisher.kafka.publisher.OutboxEventPublisher;
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
public class OutboxEventPublisherImpl implements OutboxEventPublisher {

    private static final int SEND_TIMEOUT_SECONDS = 10;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxPublisherProperties properties;
    private final OutboxEventEnvelopeFactory envelopeFactory;

    @Override
    public void publish(EventOutboxEntity event) throws Exception {
        ProducerRecord<String, String> record = createKafkaRecord(event);
        sendRecord(record);
    }

    private ProducerRecord<String, String> createKafkaRecord(EventOutboxEntity event) {
        OutboxEventEnvelope envelope = buildEnvelope(event);
        ProducerRecord<String, String> record = buildProducerRecord(event, envelope);

        addOutboxHeaders(record, event);
        return record;
    }

    private ProducerRecord<String, String> buildProducerRecord(EventOutboxEntity event, OutboxEventEnvelope envelope) {
        return new ProducerRecord<>(
                eventTopic(),
                kafkaKeyFor(event),
                messageBodyFor(envelope)
        );
    }

    private OutboxEventEnvelope buildEnvelope(EventOutboxEntity event) {
        return envelopeFactory.build(event);
    }

    private String kafkaKeyFor(EventOutboxEntity event) {
        return event.getRideRequestIdentifier();
    }

    private String eventTopic() {
        return properties.eventTopic();
    }

    private String messageBodyFor(OutboxEventEnvelope envelope) {
        return envelopeFactory.toJson(envelope);
    }

    private void addOutboxHeaders(ProducerRecord<String, String> record, EventOutboxEntity event) {
        addHeader(record, "eventId", String.valueOf(event.getId()));
        addHeader(record, "eventType", event.getEventType());
        addHeader(record, "rideRequestIdentifier", event.getRideRequestIdentifier());
    }

    private void sendRecord(ProducerRecord<String, String> record) throws Exception {
        kafkaTemplate.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private void addHeader(ProducerRecord<String, String> record, String name, String value) {
        if (value != null) {
            record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
