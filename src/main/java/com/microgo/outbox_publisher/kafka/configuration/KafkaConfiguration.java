package com.microgo.outbox_publisher.kafka.configuration;

import com.microgo.outbox_publisher.configuration.OutboxPublisherProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
@ConditionalOnProperty(name = "kafka.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaConfiguration {

    @Bean
    public KafkaAdmin kafkaAdmin(@Value("${kafka.bootstrap-servers}") String bootstrapServers) {
        return new KafkaAdmin(bootstrapServerConfig(bootstrapServers));
    }

    @Bean
    public NewTopic outboxEventsTopic(OutboxPublisherProperties properties) {
        return new NewTopic(properties.eventTopic(), properties.eventTopicPartitions(), properties.replicationFactor());
    }

    @Bean
    public NewTopic outboxEventsAckTopic(OutboxPublisherProperties properties) {
        return new NewTopic(properties.ackTopic(), properties.ackTopicPartitions(), properties.replicationFactor());
    }

    @Bean
    public ProducerFactory<String, String> outboxProducerFactory(@Value("${kafka.bootstrap-servers}") String bootstrapServers) {
        return new DefaultKafkaProducerFactory<>(producerConfig(bootstrapServers));
    }

    @Bean
    public KafkaTemplate<String, String> outboxKafkaTemplate(ProducerFactory<String, String> outboxProducerFactory) {
        return new KafkaTemplate<>(outboxProducerFactory);
    }

    @Bean
    public ConsumerFactory<String, String> outboxAckConsumerFactory(@Value("${kafka.bootstrap-servers}") String bootstrapServers) {
        return new DefaultKafkaConsumerFactory<>(consumerConfig(bootstrapServers));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> outboxAckKafkaListenerContainerFactory(
            ConsumerFactory<String, String> outboxAckConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(outboxAckConsumerFactory);
        return factory;
    }

    private Map<String, Object> producerConfig(String bootstrapServers) {
        Map<String, Object> config = new HashMap<>();
        config.putAll(bootstrapServerConfig(bootstrapServers));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return config;
    }

    private Map<String, Object> consumerConfig(String bootstrapServers) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return config;
    }

    private Map<String, Object> bootstrapServerConfig(String bootstrapServers) {
        return Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }
}
