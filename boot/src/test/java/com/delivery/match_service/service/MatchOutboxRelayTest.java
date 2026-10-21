package com.delivery.match_service.service;

import com.delivery.match_service.entity.MatchOutboxEvent;
import com.delivery.match_service.repository.MatchOutboxEventRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
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
class MatchOutboxRelayTest {

    @Mock
    private MatchOutboxEventRepository repository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private MatchOutboxRelay relay;

    @BeforeEach
    void setUp() {
        relay = new MatchOutboxRelay(repository, kafkaTemplate);
        ReflectionTestUtils.setField(relay, "batchSize", 10);
        ReflectionTestUtils.setField(relay, "sendTimeoutSeconds", 1L);
        ReflectionTestUtils.setField(relay, "maxAttempts", 3);
    }

    @Test
    @SuppressWarnings("unchecked")
    void relayPublishesRawDurablePayloadAndMarksResultSent() {
        MatchOutboxEvent event = pendingEvent();
        when(repository.lockNextOrderedBatch(10)).thenReturn(List.of(event));
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(org.mockito.Mockito.mock(SendResult.class)));

        relay.relayResults();

        ArgumentCaptor<ProducerRecord<String, String>> sent = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(sent.capture());
        assertThat(sent.getValue().topic()).isEqualTo("shipper.found");
        assertThat(sent.getValue().key()).isEqualTo("456");
        assertThat(sent.getValue().value()).contains("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        assertThat(event.getStatus()).isEqualTo(MatchOutboxEvent.Status.SENT);
        assertThat(event.getSentAt()).isNotNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    void relayKeepsFailureDurableThenMarksDeadAtConfiguredLimit() {
        MatchOutboxEvent event = pendingEvent();
        when(repository.lockNextOrderedBatch(10)).thenReturn(List.of(event));
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));

        relay.relayResults();
        assertThat(event.getStatus()).isEqualTo(MatchOutboxEvent.Status.PENDING);
        assertThat(event.getAttempts()).isEqualTo(1);
        assertThat(event.getNextAttemptAt()).isAfter(event.getCreatedAt());

        relay.relayResults();
        relay.relayResults();
        assertThat(event.getStatus()).isEqualTo(MatchOutboxEvent.Status.DEAD);
        assertThat(event.getAttempts()).isEqualTo(3);
    }

    private MatchOutboxEvent pendingEvent() {
        MatchOutboxEvent event = new MatchOutboxEvent();
        event.setEventId(UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"));
        event.setCommandEventId(UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"));
        event.setAggregateId("456");
        event.setEventType("shipper.found");
        event.setTopic("shipper.found");
        event.setEventKey("456");
        event.setPayload("{\"eventId\":\"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\"}");
        event.setStatus(MatchOutboxEvent.Status.PENDING);
        event.setAttempts(0);
        event.setCreatedAt(LocalDateTime.now().minusSeconds(1));
        event.setNextAttemptAt(LocalDateTime.now().minusSeconds(1));
        return event;
    }
}
