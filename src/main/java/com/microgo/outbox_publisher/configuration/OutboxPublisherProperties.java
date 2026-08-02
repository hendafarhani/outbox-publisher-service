package com.microgo.outbox_publisher.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * This service's own Kafka identity and tuning.
 *
 * <p>Topic names deliberately live in {@link KafkaTopicProperties} instead.
 * A topic name is an agreement with dashboard-service on the other end of the
 * channel; everything here must DIFFER per service, in particular the ack group
 * id, since Kafka splits a topic between members of one consumer group.
 */
@ConfigurationProperties(prefix = "outbox.publisher")
public record OutboxPublisherProperties(
        Integer eventTopicPartitions,
        Integer ackTopicPartitions,
        Short replicationFactor,
        Integer batchSize,
        Long fixedDelayMs,
        Long ackTimeoutSeconds,
        Integer maxRetryCount
) {

    public OutboxPublisherProperties {
        eventTopicPartitions = eventTopicPartitions == null ? 3 : eventTopicPartitions;
        ackTopicPartitions = ackTopicPartitions == null ? 3 : ackTopicPartitions;
        replicationFactor = replicationFactor == null ? 1 : replicationFactor;
        batchSize = batchSize == null ? 50 : batchSize;
        fixedDelayMs = fixedDelayMs == null ? 1000L : fixedDelayMs;
        ackTimeoutSeconds = ackTimeoutSeconds == null ? 30L : ackTimeoutSeconds;
        maxRetryCount = maxRetryCount == null ? 10 : maxRetryCount;
    }
}
