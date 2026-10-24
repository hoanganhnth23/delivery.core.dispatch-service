package com.delivery.saga_orchestrator_service.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.retrytopic.DestinationTopic;
import org.springframework.kafka.retrytopic.SuffixingRetryTopicNamesProviderFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaConfigTest {

    @Test
    void usesConfiguredBootstrapServersForProducerAndConsumer() {
        KafkaConfig config = new KafkaConfig("kafka:19092", false);

        assertThat(config.producerFactory().getConfigurationProperties())
                .containsEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:19092");
        assertThat(config.consumerFactory().getConfigurationProperties())
                .containsEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:19092");
    }

    @Test
    void usesConfiguredGroupAndListenerStartupPolicy() {
        KafkaConfig config = new KafkaConfig("kafka:19092", "saga-b8-status", false);
        var factory = config.kafkaListenerContainerFactory(
                config.sagaKafkaErrorHandler(config.sagaDeadLetterRecoverer(mock(KafkaTemplate.class))));

        assertThat(config.consumerFactory().getConfigurationProperties())
                .containsEntry(ConsumerConfig.GROUP_ID_CONFIG, "saga-b8-status");
        assertThat(ReflectionTestUtils.getField(factory, "autoStartup")).isEqualTo(false);
    }

    @Test
    void retryEndpointWithoutListenerGroupKeepsFactoryConsumerGroup() {
        KafkaConfig config = new KafkaConfig("kafka:19092", "saga-legacy-group", false);
        MethodKafkaListenerEndpoint<String, String> endpoint = new MethodKafkaListenerEndpoint<>();
        endpoint.setId("saga-listener");
        DestinationTopic.Properties retryProperties = mock(DestinationTopic.Properties.class);
        when(retryProperties.suffix()).thenReturn("-retry-1000");

        String retryEndpointGroup = new SuffixingRetryTopicNamesProviderFactory()
                .createRetryTopicNamesProvider(retryProperties)
                .getGroupId(endpoint);

        // @KafkaListener has no groupId, therefore retry-topic registration has
        // no endpoint override to suffix. The ConsumerFactory group-id is the
        // actual group inherited by the retry container.
        assertThat(retryEndpointGroup).isNull();
        assertThat(config.consumerFactory().getConfigurationProperties())
                .containsEntry(ConsumerConfig.GROUP_ID_CONFIG, "saga-legacy-group");
    }

    @Test
    @SuppressWarnings("unchecked")
    void poisonSagaEventIsPublishedUnchangedToSamePartitionDlt() {
        KafkaConfig config = new KafkaConfig("kafka:19092", true);
        KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
        when(template.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
        var recoverer = config.sagaDeadLetterRecoverer(template);
        ConsumerRecord<String, String> source = new ConsumerRecord<>(
                "delivery.created.result", 2, 9L, "order-101", "bad-json");

        recoverer.accept(source, new IllegalArgumentException("poison"));

        ArgumentCaptor<ProducerRecord<String, String>> sent =
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(sent.capture());
        assertThat(sent.getValue().topic()).isEqualTo("delivery.created.result.saga.DLT");
        assertThat(sent.getValue().partition()).isEqualTo(2);
        assertThat(sent.getValue().key()).isEqualTo("order-101");
        assertThat(sent.getValue().value()).isEqualTo("bad-json");
    }

    @Test
    @SuppressWarnings("unchecked")
    void retryTopicFallbackReturnsToTheCanonicalSagaDlt() {
        KafkaConfig config = new KafkaConfig("kafka:19092", true);
        KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
        when(template.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
        var recoverer = config.sagaDeadLetterRecoverer(template);
        ConsumerRecord<String, String> retry = new ConsumerRecord<>(
                "delivery.created.result-retry-saga-1000", 2, 9L, "order-101", "bad-json");

        recoverer.accept(retry, new IllegalArgumentException("poison"));

        ArgumentCaptor<ProducerRecord<String, String>> sent =
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(sent.capture());
        assertThat(sent.getValue().topic()).isEqualTo("delivery.created.result.saga.DLT");
        assertThat(sent.getValue().partition()).isEqualTo(2);
    }

    @Test
    void errorHandlerCommitsOnlyAfterFiniteRetryRecovery() {
        KafkaConfig config = new KafkaConfig("kafka:19092", true);
        var recoverer = config.sagaDeadLetterRecoverer(mock(KafkaTemplate.class));
        var handler = config.sagaKafkaErrorHandler(recoverer);

        assertThat(handler.isAckAfterHandle()).isTrue();
        assertThat(handler.seeksAfterHandling()).isTrue();
    }
}
