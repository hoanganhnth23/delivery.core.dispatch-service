package com.delivery.saga_orchestrator_service.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.springframework.util.backoff.FixedBackOff;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * ✅ Kafka Config — consume raw String, parse JSON manually
 * Tránh bị ép kiểu type header từ các service khác nhau
 */
@Configuration
@EnableKafka
public class KafkaConfig {

    private final String bootstrapServers;
    private final String groupId;
    private final boolean listenerAutoStartup;

    @Autowired
    public KafkaConfig(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id:saga-orchestrator}") String groupId,
            @Value("${spring.kafka.listener.auto-startup:true}") boolean listenerAutoStartup) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.listenerAutoStartup = listenerAutoStartup;
    }

    KafkaConfig(String bootstrapServers, boolean listenerAutoStartup) {
        this(bootstrapServers, "saga-orchestrator", listenerAutoStartup);
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean("retryKafkaTemplate")
    public KafkaTemplate<String, String> retryKafkaTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            DefaultErrorHandler sagaKafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setAutoStartup(listenerAutoStartup);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        factory.setCommonErrorHandler(sagaKafkaErrorHandler);

        return factory;
    }

    @Bean
    public DeadLetterPublishingRecoverer sagaDeadLetterRecoverer(
            @Qualifier("retryKafkaTemplate") KafkaTemplate<String, String> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(
                        // Keep the blocking/error-handler escape path aligned
                        // with the owner-isolated @RetryableTopic DLT. If a
                        // retry container invokes the common handler, strip
                        // its delay suffix so it joins the canonical source DLT.
                        ownerDltTopic(record.topic()), record.partition()));
        recoverer.setFailIfSendResultIsError(true);
        return recoverer;
    }

    @Bean
    public DefaultErrorHandler sagaKafkaErrorHandler(
            DeadLetterPublishingRecoverer recoverer, MeterRegistry meterRegistry) {
        DefaultErrorHandler handler = new DefaultErrorHandler((record, error) -> {
            meterRegistry.counter("delivery.kafka.events", "event", "dlt").increment();
            recoverer.accept(record, error);
        }, new FixedBackOff(1000L, 2));
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        handler.setRetryListeners(new org.springframework.kafka.listener.RetryListener() {
            @Override public void failedDelivery(org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record,
                                                 Exception error, int attempt) {
                meterRegistry.counter("delivery.kafka.events", "event", "retry").increment();
            }
        });
        handler.setCommitRecovered(true);
        return handler;
    }

    // Keeps focused configuration tests independent from the application MeterRegistry.
    DefaultErrorHandler sagaKafkaErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        return sagaKafkaErrorHandler(recoverer,
                new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
    }

    private static String ownerDltTopic(String topic) {
        return topic.replaceFirst("-retry-saga-\\d+$", "") + ".saga.DLT";
    }
}
