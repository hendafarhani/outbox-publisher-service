package com.microgo.outbox_publisher.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microgo.outbox_publisher.configuration.OutboxPublisherProperties;
import com.microgo.outbox_publisher.entity.EventOutboxEntity;
import com.microgo.outbox_publisher.enums.OutboxEventStatus;
import com.microgo.outbox_publisher.repository.EventOutboxRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = {
        "spring.cloud.config.enabled=false",
        "spring.cloud.discovery.enabled=false",
        "eureka.client.enabled=false",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "outbox.publisher.fixed-delay-ms=200",
        "outbox.publisher.ack-timeout-seconds=30"
})
@Testcontainers(disabledWithoutDocker = true)
class OutboxPublisherKafkaIntegrationTest {

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.4");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    @Autowired
    private EventOutboxRepository eventOutboxRepository;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private OutboxPublisherProperties properties;

    @DynamicPropertySource
    static void registerContainerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", mysql::getJdbcUrl);
        registry.add("spring.datasource.username", mysql::getUsername);
        registry.add("spring.datasource.password", mysql::getPassword);
        registry.add("spring.datasource.driver-class-name", mysql::getDriverClassName);
        registry.add("kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @BeforeEach
    void cleanState() {
        eventOutboxRepository.deleteAll();
    }

    @Test
    void publishesPendingOutboxEventAndProcessesDashboardAck() throws Exception {
        EventOutboxEntity event = event("ride-flow", OffsetDateTime.parse("2026-06-13T10:00:00Z"));
        eventOutboxRepository.save(event);

        ConsumerRecord<String, String> record = consumeOneEvent();

        assertThat(record.key()).isEqualTo("ride-flow");
        assertThat(record.headers().lastHeader("eventId")).isNotNull();
        JsonNode envelope = objectMapper.readTree(record.value());
        Long eventId = envelope.get("eventId").asLong();
        assertThat(envelope.get("eventType").asText()).isEqualTo("RIDER_NOTIFIED");
        assertThat(envelope.get("rideRequestIdentifier").asText()).isEqualTo("ride-flow");
        assertThat(envelope.get("payload").get("rideRequestId").asLong()).isEqualTo(42L);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(eventOutboxRepository.findById(eventId)).get()
                        .extracting(EventOutboxEntity::getStatus)
                        .isEqualTo(OutboxEventStatus.PUBLISHED));

        kafkaTemplate.send(
                properties.ackTopic(),
                String.valueOf(eventId),
                """
                {"eventId":%d,"status":"WEBSOCKET_PUBLISHED","processedAt":"2026-06-13T10:00:03Z","service":"dashboard-service"}
                """.formatted(eventId)
        ).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(eventOutboxRepository.findById(eventId)).get()
                        .extracting(EventOutboxEntity::getStatus)
                        .isEqualTo(OutboxEventStatus.PROCESSED));
    }

    @Test
    void createsTopicsWithConfiguredPartitions() throws Exception {
        try (AdminClient adminClient = AdminClient.create(Map.of(
                "bootstrap.servers", kafka.getBootstrapServers()
        ))) {
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                Map<String, Integer> partitions = adminClient.describeTopics(List.of(
                                properties.eventTopic(),
                                properties.ackTopic()
                        ))
                        .allTopicNames()
                        .get()
                        .entrySet()
                        .stream()
                        .collect(java.util.stream.Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> entry.getValue().partitions().size()
                        ));
                assertThat(partitions.get(properties.eventTopic())).isEqualTo(3);
                assertThat(partitions.get(properties.ackTopic())).isEqualTo(3);
            });
        }
    }

    @Test
    void preservesOrderingForSameRideRequestIdentifier() throws Exception {
        OffsetDateTime now = OffsetDateTime.parse("2026-06-13T10:00:00Z");
        eventOutboxRepository.save(event("ride-order", now));
        eventOutboxRepository.save(event("ride-order", now.plusSeconds(1)));

        List<ConsumerRecord<String, String>> records = consumeEvents(2);

        assertThat(records).extracting(ConsumerRecord::key).containsExactly("ride-order", "ride-order");
        assertThat(records.get(0).value()).contains("\"eventTimestamp\":\"2026-06-13T10:00:00Z\"");
        assertThat(records.get(1).value()).contains("\"eventTimestamp\":\"2026-06-13T10:00:01Z\"");
        assertThat(records.get(0).partition()).isEqualTo(records.get(1).partition());
    }

    private ConsumerRecord<String, String> consumeOneEvent() {
        return consumeEvents(1).getFirst();
    }

    private List<ConsumerRecord<String, String>> consumeEvents(int expectedCount) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties())) {
            consumer.subscribe(List.of(properties.eventTopic()));
            java.util.ArrayList<ConsumerRecord<String, String>> records = new java.util.ArrayList<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (records.size() < expectedCount && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(500))
                        .records(properties.eventTopic())
                        .forEach(records::add);
            }
            assertThat(records).hasSizeGreaterThanOrEqualTo(expectedCount);
            return records.stream().limit(expectedCount).toList();
        }
    }

    private Properties consumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "outbox-test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    private EventOutboxEntity event(String rideRequestIdentifier, OffsetDateTime createdAt) {
        return EventOutboxEntity.builder()
                .eventType("RIDER_NOTIFIED")
                .status(OutboxEventStatus.PENDING)
                .rideRequestId(42L)
                .rideRequestIdentifier(rideRequestIdentifier)
                .requesterId("user-flow")
                .riderId("rider-flow")
                .payload("""
                        {"eventType":"RIDER_NOTIFIED","eventTimestamp":"%s","rideRequestId":42,"rideRequestIdentifier":"%s","requesterId":"user-flow","riderId":"rider-flow","rideStatus":"PENDING"}
                        """.formatted(createdAt, rideRequestIdentifier))
                .createdAt(createdAt)
                .updatedAt(createdAt)
                .build();
    }
}
