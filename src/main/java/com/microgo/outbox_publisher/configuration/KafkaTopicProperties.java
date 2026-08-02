package com.microgo.outbox_publisher.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Topic names, bound from the fleet-wide microgo.topics.* keys defined once in
 * centralized-configuration/application.properties.
 *
 * <p>Separate from {@link OutboxPublisherProperties} on purpose. This service
 * produces to the same two topics that dashboard-service consumes, so these
 * names must be identical in both. Everything left in OutboxPublisherProperties
 * is the opposite: this service's own ack group id, listener id, partition
 * counts and batching, which must NOT be shared.
 */
@ConfigurationProperties(prefix = "microgo.topics")
public record KafkaTopicProperties(
        String rideRequestEvents,
        String rideRequestEventsAcks
) {
}
