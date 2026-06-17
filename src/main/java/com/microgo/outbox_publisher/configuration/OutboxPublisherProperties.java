package com.microgo.outbox_publisher.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "outbox.publisher")
public record OutboxPublisherProperties(
        String eventTopic,
        String ackTopic,
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
