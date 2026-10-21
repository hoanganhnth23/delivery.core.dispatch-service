package com.delivery.match_service.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaConfigTest {

    @Test
    void usesConfiguredBootstrapServersForProducerAndConsumer() {
        KafkaConfig config = new KafkaConfig("kafka:19092", "match-proof");

        assertThat(config.producerFactory().getConfigurationProperties())
                .containsEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:19092");
        assertThat(config.consumerFactory().getConfigurationProperties())
                .containsEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:19092");
        assertThat(config.consumerFactory().getConfigurationProperties())
                .containsEntry(ConsumerConfig.GROUP_ID_CONFIG, "match-proof");
    }

    @Test
    void isolatesOutOfOrderAcknowledgmentsToMonoListenerFactory() {
        KafkaConfig config = new KafkaConfig("kafka:19092", "match-proof");
        var recoverer = config.matchDeadLetterRecoverer(mock(KafkaTemplate.class));
        var errorHandler = config.matchKafkaErrorHandler(recoverer);

        var projectionProperties = config.kafkaListenerContainerFactory(errorHandler)
                .getContainerProperties();
        var reactiveProperties = config.reactiveKafkaListenerContainerFactory(errorHandler)
                .getContainerProperties();
        assertThat(projectionProperties.getAckMode())
                .isEqualTo(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        assertThat(projectionProperties.isAsyncAcks()).isFalse();
        assertThat(reactiveProperties.getAckMode())
                .isEqualTo(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        assertThat(reactiveProperties.isAsyncAcks()).isTrue();
        assertThat(config.locationKafkaListenerContainerFactory(errorHandler)
                .getContainerProperties().isObservationEnabled()).isFalse();
    }

    @Test
    @SuppressWarnings("unchecked")
    void poisonProjectionEventIsPublishedUnchangedToSamePartitionDlt() {
        KafkaConfig config = new KafkaConfig("kafka:19092", "match-proof");
        KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
        when(template.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
        var recoverer = config.matchDeadLetterRecoverer(template);
        ConsumerRecord<String, String> source = new ConsumerRecord<>(
                "shipper.status-change", 5, 11L, "shipper-7", "bad-json");

        recoverer.accept(source, new IllegalArgumentException("poison"));

        ArgumentCaptor<ProducerRecord<String, String>> sent =
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(sent.capture());
        assertThat(sent.getValue().topic()).isEqualTo("shipper.status-change.DLT");
        assertThat(sent.getValue().partition()).isEqualTo(5);
        assertThat(sent.getValue().key()).isEqualTo("shipper-7");
        assertThat(sent.getValue().value()).isEqualTo("bad-json");
    }

    @Test
    void commonErrorHandlerCommitsOnlyAfterFiniteRetryRecovery() {
        KafkaConfig config = new KafkaConfig("kafka:19092", "match-proof");
        var recoverer = config.matchDeadLetterRecoverer(mock(KafkaTemplate.class));
        var handler = config.matchKafkaErrorHandler(recoverer);

        assertThat(handler.isAckAfterHandle()).isTrue();
        assertThat(handler.seeksAfterHandling()).isTrue();
    }
}
