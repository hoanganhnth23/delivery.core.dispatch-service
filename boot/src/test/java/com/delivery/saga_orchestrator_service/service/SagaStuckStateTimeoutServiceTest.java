package com.delivery.saga_orchestrator_service.service;

import com.delivery.saga_orchestrator_service.entity.SagaInstance;
import com.delivery.saga_orchestrator_service.entity.SagaOutboxEvent;
import com.delivery.saga_orchestrator_service.repository.SagaInstanceRepository;
import com.delivery.saga_orchestrator_service.repository.SagaOutboxEventRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class SagaStuckStateTimeoutServiceTest {
    private final SagaInstanceRepository cases = mock(SagaInstanceRepository.class);
    private final SagaOutboxEventRepository outbox = mock(SagaOutboxEventRepository.class);
    private final SagaStuckStateTimeoutService service = new SagaStuckStateTimeoutService(cases, outbox, new SimpleMeterRegistry());

    @Test
    void staleStateAndFreshReplyWindowHaveNoSideEffects() {
        when(cases.findByOrderIdForUpdate(1L)).thenReturn(Optional.empty());
        expire();
        var saga = saga();
        saga.setStatus(SagaInstance.SagaStatus.SHIPPER_ASSIGNED);
        when(cases.findByOrderIdForUpdate(1L)).thenReturn(Optional.of(saga));
        expire();
        saga.setStatus(SagaInstance.SagaStatus.OFFER_PERSISTING);
        expire();
        verifyNoInteractions(outbox);
        verify(cases, never()).save(any());
    }

    @Test
    void activeLeaseIsPreservedButExpiredLeaseIsRequeued() {
        var saga = saga();
        var command = new SagaOutboxEvent();
        command.setId(5L);
        UUID identity = UUID.randomUUID();
        command.setEventId(identity);
        command.setPayload("original payload");
        command.setStatus(SagaOutboxEvent.Status.IN_FLIGHT);
        command.setLeaseToken(UUID.randomUUID());
        command.setLeaseUntil(LocalDateTime.now().plusSeconds(30));
        when(cases.findByOrderIdForUpdate(1L)).thenReturn(Optional.of(saga));
        when(outbox.findFirstByAggregateIdAndTopicOrderByIdDesc("1", SagaManager.CMD_CACHE_SHIPPER_FOUND))
                .thenReturn(Optional.of(command));
        when(outbox.findByIdForUpdate(5L)).thenReturn(Optional.of(command));
        expire();
        assertThat(saga.getStuckResendAttempts()).isZero();
        verify(outbox, never()).save(any());
        command.setLeaseUntil(LocalDateTime.now().minusSeconds(1));
        expire();
        assertThat(saga.getStuckResendAttempts()).isOne();
        assertThat(command.getStatus()).isEqualTo(SagaOutboxEvent.Status.PENDING);
        assertThat(command.getLeaseToken()).isNull();
        assertThat(command.getEventId()).isEqualTo(identity);
        assertThat(command.getPayload()).isEqualTo("original payload");
    }

    @Test
    void stateChangeResetsRetryBookkeepingButSameStateDoesNot() {
        var saga = saga();
        var entered = saga.getStateEnteredAt();
        saga.setStuckResendAttempts(2);
        saga.setStuckLastResentAt(LocalDateTime.now());
        saga.setStatus(SagaInstance.SagaStatus.OFFER_PERSISTING);
        assertThat(saga.getStateEnteredAt()).isEqualTo(entered);
        assertThat(saga.getStuckResendAttempts()).isEqualTo(2);
        saga.setStatus(SagaInstance.SagaStatus.OFFER_RETIRING);
        assertThat(saga.getStateEnteredAt()).isAfter(entered);
        assertThat(saga.getStuckLastResentAt()).isNull();
        assertThat(saga.getStuckResendAttempts()).isZero();
    }

    private SagaInstance saga() {
        var saga = new SagaInstance();
        saga.setId(UUID.randomUUID());
        saga.setOrderId(1L);
        saga.setStatus(SagaInstance.SagaStatus.OFFER_PERSISTING);
        saga.setStateEnteredAt(LocalDateTime.now().minusSeconds(121));
        return saga;
    }

    private void expire() {
        service.expire(1L, SagaInstance.SagaStatus.OFFER_PERSISTING, Duration.ofSeconds(120), 3,
                SagaManager.CMD_CACHE_SHIPPER_FOUND);
    }
}
