package com.delivery.match_service.service;

import com.delivery.match_service.entity.MatchOutboxEvent;
import com.delivery.match_service.repository.MatchOutboxEventRepository;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SuppressWarnings({"unchecked", "rawtypes"})
class MatchOutboxLeasedRelayTest {
    private final MatchOutboxEventRepository repository = mock(MatchOutboxEventRepository.class);
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final MatchOutboxLeaseService leases = mock(MatchOutboxLeaseService.class);
    private final MatchOutboxRelay relay = new MatchOutboxRelay(repository, kafka);

    MatchOutboxLeasedRelayTest() {
        ReflectionTestUtils.setField(relay, "leaseService", leases);
        ReflectionTestUtils.setField(relay, "batchSize", 2);
        ReflectionTestUtils.setField(relay, "sendTimeoutSeconds", 1L);
        ReflectionTestUtils.setField(relay, "leaseSeconds", 0L);
        ReflectionTestUtils.setField(relay, "maxAttempts", 3);
    }

    @Test
    void publishedDurableEventCarriesMetadataAndAcknowledgesWithItsOwnLease() {
        MatchOutboxEvent event = event();
        when(leases.claim(eq(1), any(), eq(11L))).thenReturn(List.of(event), List.of());
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        when(leases.markSent(eq(1L), any())).thenReturn(true);
        relay.relayResults();
        ArgumentCaptor<ProducerRecord<String, String>> sent = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafka).send(sent.capture());
        ProducerRecord<String, String> record = sent.getValue();
        assertThat(record.topic()).isEqualTo("test.topic");
        assertThat(record.key()).isEqualTo("42");
        assertThat(record.value().toString()).isEqualTo("{\"orderId\":42}");
        assertThat(new String(record.headers().lastHeader("eventId").value(), StandardCharsets.UTF_8))
                .isEqualTo(event.getEventId().toString());
        assertThat(new String(record.headers().lastHeader("eventType").value(), StandardCharsets.UTF_8)).isEqualTo("TEST");
        assertThat(new String(record.headers().lastHeader("aggregateId").value(), StandardCharsets.UTF_8)).isEqualTo("42");
        ArgumentCaptor<UUID> tokens = ArgumentCaptor.forClass(UUID.class);
        verify(leases, times(2)).claim(eq(1), tokens.capture(), eq(11L));
        verify(leases).markSent(1L, tokens.getAllValues().get(0));
        assertThat(tokens.getAllValues()).doesNotHaveDuplicates();
        verify(leases, never()).markFailure(any(), any(), any(), anyInt());
        verifyNoInteractions(repository);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void publishFailureRemainsRecoverableEvenWhenFailureUpdateLosesLeaseOrThrows(boolean throwsOnUpdate) {
        MatchOutboxEvent event = event();
        when(leases.claim(eq(1), any(), anyLong())).thenReturn(List.of(event), List.of());
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));
        if (throwsOnUpdate) when(leases.markFailure(eq(1L), any(), any(), eq(3))).thenThrow(new RuntimeException("database down"));
        else when(leases.markFailure(eq(1L), any(), any(), eq(3))).thenReturn(false);
        relay.relayResults();
        ArgumentCaptor<Throwable> failure = ArgumentCaptor.forClass(Throwable.class);
        verify(leases).markFailure(eq(1L), any(), failure.capture(), eq(3));
        assertThat(failure.getValue()).hasCauseInstanceOf(RuntimeException.class);
        verify(leases, never()).markSent(any(), any());
        verify(leases, times(2)).claim(eq(1), any(), anyLong());
        verifyNoInteractions(repository);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void successfulPublishWithLostOrFailedStateUpdateIsLeftForReclaim(boolean throwsOnUpdate) {
        when(leases.claim(eq(1), any(), anyLong())).thenReturn(List.of(event()), List.of());
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        if (throwsOnUpdate) when(leases.markSent(eq(1L), any())).thenThrow(new RuntimeException("database down"));
        else when(leases.markSent(eq(1L), any())).thenReturn(false);
        relay.relayResults();
        verify(leases).markSent(eq(1L), any());
        verify(leases, never()).markFailure(any(), any(), any(), anyInt());
        verifyNoInteractions(repository);
    }

    @Test
    void failedOrNullClaimStopsWithoutPublishingOrUsingLegacyRepository() {
        when(leases.claim(eq(1), any(), anyLong())).thenThrow(new RuntimeException("database down"));
        relay.relayResults();
        when(leases.claim(eq(1), any(), anyLong())).thenReturn(null);
        relay.relayResults();
        verifyNoInteractions(kafka, repository);
        verify(leases, never()).markSent(any(), any());
        verify(leases, never()).markFailure(any(), any(), any(), anyInt());
    }

    @Test
    void interruptionRecordsFailureRestoresInterruptAndStopsClaiming() throws Exception {
        when(leases.claim(eq(1), any(), anyLong())).thenReturn(List.of(event()));
        CompletableFuture future = mock(CompletableFuture.class);
        when(future.get(1L, TimeUnit.SECONDS)).thenThrow(new InterruptedException("shutdown"));
        when(kafka.send(any(ProducerRecord.class))).thenReturn(future);
        when(leases.markFailure(eq(1L), any(), any(), eq(3))).thenReturn(true);
        try {
            relay.relayResults();
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            verify(leases).claim(eq(1), any(), anyLong());
            verify(leases).markFailure(eq(1L), any(), isA(InterruptedException.class), eq(3));
            verify(leases, never()).markSent(any(), any());
        } finally { Thread.interrupted(); }
    }

    private MatchOutboxEvent event() {
        MatchOutboxEvent event = new MatchOutboxEvent();
        event.setId(1L); event.setEventId(UUID.randomUUID()); event.setTopic("test.topic");
        event.setEventType("TEST"); event.setAggregateId("42"); event.setEventKey("42");
        event.setPayload("{\"orderId\":42}");
        return event;
    }
}
