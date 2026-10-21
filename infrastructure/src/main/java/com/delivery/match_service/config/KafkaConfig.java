package com.delivery.match_service.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.EnableKafkaRetryTopic;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import com.delivery.match_service.metrics.BusinessMetrics;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * ✅ Kafka Configuration cho Match Service theo Backend Instructions
 * Consumer configuration để nhận FindShipperEvent từ Delivery Service
 * Producer configuration để publish MatchEvent đến Notification Service
 */
@Configuration
@EnableKafkaRetryTopic
public class KafkaConfig {
    
    private final String bootstrapServers;
    private final String consumerGroupId;

    public KafkaConfig(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id:match-service}") String consumerGroupId) {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroupId = consumerGroupId;
    }
    
    // ✅ Producer Configuration để publish events
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        
        // ✅ Producer reliability settings
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Wait for all replicas
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        
        return new DefaultKafkaProducerFactory<>(props);
    }
    
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
    
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // All Match listeners must honor the deployed consumer group. A
        // hard-coded value would make independently booted replicas consume
        // durable commands outside the intended group during a rollout.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        
        // A rebuilt consumer group must replay durable commands/status events.
        // Location events carry their own freshness fence in the listener.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        
        return new DefaultKafkaConsumerFactory<>(props);
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            DefaultErrorHandler matchKafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
                new ConcurrentKafkaListenerContainerFactory<>();
        
        factory.setConsumerFactory(consumerFactory());
        
        // ✅ Manual acknowledgment for reliable processing
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(matchKafkaErrorHandler);
        
        return factory;
    }

    /** Location pings are a high-volume Redis projection, not an order trace boundary. */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> locationKafkaListenerContainerFactory(
            DefaultErrorHandler matchKafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setObservationEnabled(false);
        factory.setCommonErrorHandler(matchKafkaErrorHandler);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> reactiveKafkaListenerContainerFactory(
            DefaultErrorHandler matchKafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // Only the Mono-returning match command needs deferred/out-of-order ACKs.
        // Projection listeners must use normal error-handler seek/retry semantics.
        factory.getContainerProperties().setAsyncAcks(true);
        factory.setCommonErrorHandler(matchKafkaErrorHandler);
        return factory;
    }

    @Bean
    public DeadLetterPublishingRecoverer matchDeadLetterRecoverer(
            KafkaTemplate<String, String> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(
                        record.topic() + ".DLT", record.partition()));
        recoverer.setFailIfSendResultIsError(true);
        return recoverer;
    }

    @Bean
    public DefaultErrorHandler matchKafkaErrorHandler(
            DeadLetterPublishingRecoverer recoverer, BusinessMetrics businessMetrics) {
        DefaultErrorHandler handler = new DefaultErrorHandler((record, error) -> {
            businessMetrics.kafka("dlt");
            recoverer.accept(record, error);
        }, new FixedBackOff(1000L, 3));
        handler.setRetryListeners(new org.springframework.kafka.listener.RetryListener() {
            @Override public void failedDelivery(org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record,
                                                 Exception error, int attempt) { businessMetrics.kafka("retry"); }
        });
        handler.setCommitRecovered(true);
        return handler;
    }

    /** Compatibility helper for direct configuration tests; Spring injection uses the overload above. */
    DefaultErrorHandler matchKafkaErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        return matchKafkaErrorHandler(recoverer,
                new BusinessMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry()));
    }

    @Bean
    public TaskScheduler retryTopicTaskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("match-retry-topic-");
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        return scheduler;
    }
}
