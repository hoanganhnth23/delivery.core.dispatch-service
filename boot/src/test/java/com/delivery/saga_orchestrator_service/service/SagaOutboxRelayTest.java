package com.delivery.saga_orchestrator_service.service;

import com.delivery.saga_orchestrator_service.entity.SagaOutboxEvent;
import com.delivery.saga_orchestrator_service.repository.SagaOutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SagaOutboxRelayTest {

    @Mock SagaOutboxEventRepository repository;
    @Mock KafkaTemplate<String, Object> kafkaTemplate;
    private SagaOutboxRelay relay;

    @BeforeEach
    void setUp() {
        relay = new SagaOutboxRelay(repository, kafkaTemplate, new ObjectMapper());
        ReflectionTestUtils.setField(relay, "batchSize", 10);
        ReflectionTestUtils.setField(relay, "sendTimeoutSeconds", 1L);
        ReflectionTestUtils.setField(relay, "maxAttempts", 2);
    }

    @Test
    void successfulSendMarksCommandSentAndForwardsStableMetadata() {
        SagaOutboxEvent event = pendingEvent();
        when(repository.lockNextOrderedBatch(10)).thenReturn(List.of(event));
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        relay.relayCommands();

        assertThat(event.getStatus()).isEqualTo(SagaOutboxEvent.Status.SENT);
        assertThat(event.getSentAt()).isNotNull();
        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo(SagaManager.CMD_CREATE_DELIVERY);
        assertThat(captor.getValue().key()).isEqualTo("42");
        assertThat(new String(captor.getValue().headers().lastHeader("eventId").value()))
                .isEqualTo(event.getEventId().toString());
        verify(repository).save(event);
    }

    @Test
    void failureRetriesThenMarksDead() {
        SagaOutboxEvent event = pendingEvent();
        when(repository.lockNextOrderedBatch(10)).thenReturn(List.of(event));
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("Kafka unavailable")));

        relay.relayCommands();
        assertThat(event.getStatus()).isEqualTo(SagaOutboxEvent.Status.PENDING);
        assertThat(event.getAttempts()).isEqualTo(1);
        assertThat(event.getNextAttemptAt()).isAfter(LocalDateTime.now());

        relay.relayCommands();
        assertThat(event.getStatus()).isEqualTo(SagaOutboxEvent.Status.DEAD);
        assertThat(event.getAttempts()).isEqualTo(2);
        assertThat(event.getLastError()).contains("Kafka unavailable");
    }

    private SagaOutboxEvent pendingEvent() {
        SagaOutboxEvent event = new SagaOutboxEvent();
        event.setEventId(UUID.randomUUID());
        event.setAggregateId("42");
        event.setEventType(SagaManager.CMD_CREATE_DELIVERY);
        event.setTopic(SagaManager.CMD_CREATE_DELIVERY);
        event.setEventKey("42");
        event.setPayload("{\"orderId\":42}");
        event.setStatus(SagaOutboxEvent.Status.PENDING);
        event.setAttempts(0);
        event.setNextAttemptAt(LocalDateTime.now().minusSeconds(1));
        event.setCreatedAt(LocalDateTime.now());
        return event;
    }
}
