package com.delivery.saga_orchestrator_service.service;

import com.delivery.observability.SafeLog;
import com.delivery.saga_orchestrator_service.entity.SagaOutboxEvent;
import com.delivery.saga_orchestrator_service.repository.SagaOutboxEventRepository;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class SagaOutboxLeaseServiceTest {
    private final SagaOutboxEventRepository repository = mock(SagaOutboxEventRepository.class);
    private final SagaOutboxLeaseService service = new SagaOutboxLeaseService(repository);
    private final UUID token = UUID.randomUUID();

    @Test
    void claimBoundsBatchAndLeaseAndPersistsOwnership() {
        SagaOutboxEvent event = new SagaOutboxEvent();
        when(repository.lockNextClaimableBatch(500)).thenReturn(List.of(event));
        LocalDateTime before = LocalDateTime.now();
        assertThat(service.claim(900, token, 0)).containsExactly(event);
        assertThat(event.getStatus()).isEqualTo(SagaOutboxEvent.Status.IN_FLIGHT);
        assertThat(event.getLeaseToken()).isEqualTo(token);
        assertThat(event.getLeaseUntil()).isBetween(before.plusSeconds(5), LocalDateTime.now().plusSeconds(5));
        verify(repository).save(event);
    }

    @Test
    void emptyClaimDoesNotWriteAnyRows() {
        when(repository.lockNextClaimableBatch(1)).thenReturn(null, List.of());
        assertThat(service.claim(0, token, 20)).isEmpty();
        assertThat(service.claim(-1, token, 20)).isEmpty();
        verify(repository, never()).save(any());
    }

    @Test
    void ownedSuccessfulPublishClearsLeaseAndPreviousError() {
        SagaOutboxEvent event = owned();
        event.setLastError("previous error");
        assertThat(service.markSent(1L, token)).isTrue();
        assertThat(event.getStatus()).isEqualTo(SagaOutboxEvent.Status.SENT);
        assertThat(event.getSentAt()).isNotNull();
        assertThat(event.getLastError()).isNull();
        assertThat(event.getLeaseToken()).isNull();
        assertThat(event.getLeaseUntil()).isNull();
        verify(repository).save(event);
    }

    @Test
    void failureSchedulesBackoffThenStopsAtAttemptLimit() {
        SagaOutboxEvent event = owned();
        LocalDateTime before = LocalDateTime.now();
        assertThat(service.markFailure(1L, token, new IllegalStateException("broker unavailable"), 2)).isTrue();
        assertThat(event.getAttempts()).isEqualTo(1);
        assertThat(event.getStatus()).isEqualTo(SagaOutboxEvent.Status.PENDING);
        assertThat(event.getNextAttemptAt()).isBetween(before.plusSeconds(1), LocalDateTime.now().plusSeconds(1));
        assertThat(event.getLastError()).contains("broker unavailable");
        assertThat(event.getLeaseToken()).isNull();
        assertThat(event.getLeaseUntil()).isNull();
        event.setStatus(SagaOutboxEvent.Status.IN_FLIGHT);
        event.setLeaseToken(token);
        assertThat(service.markFailure(1L, token, new IllegalStateException("broker unavailable"), 2)).isTrue();
        assertThat(event.getAttempts()).isEqualTo(2);
        assertThat(event.getStatus()).isEqualTo(SagaOutboxEvent.Status.DEAD);
        verify(repository, times(2)).save(event);
    }

    @Test
    void failureBoundsErrorLengthAndCapsBackoff() {
        IllegalStateException failure = new IllegalStateException("x".repeat(3000));
        SagaOutboxEvent event = owned();
        event.setAttempts(20);
        LocalDateTime before = LocalDateTime.now();
        assertThat(service.markFailure(1L, token, failure, 200)).isTrue();
        // SafeLog bounds the message before the 2000-char column cap applies.
        assertThat(event.getLastError()).isEqualTo(SafeLog.exceptionMessage(failure)).hasSizeLessThan(3000);
        assertThat(event.getNextAttemptAt()).isBetween(before.plusSeconds(256), LocalDateTime.now().plusSeconds(256));
        assertThat(event.getStatus()).isEqualTo(SagaOutboxEvent.Status.PENDING);
    }

    @Test
    void missingIdentityOrStaleOwnershipCannotMutateEvent() {
        assertThat(service.markSent(null, token)).isFalse();
        assertThat(service.markFailure(1L, null, new RuntimeException(), 1)).isFalse();
        when(repository.findByIdForUpdate(1L)).thenReturn(Optional.empty());
        assertThat(service.markSent(1L, token)).isFalse();
        SagaOutboxEvent event = owned();
        assertThat(service.markSent(1L, UUID.randomUUID())).isFalse();
        event.setStatus(SagaOutboxEvent.Status.SENT);
        assertThat(service.markFailure(1L, token, new RuntimeException(), 1)).isFalse();
        assertThat(event.getAttempts()).isZero();
        verify(repository, never()).save(any());
    }

    private SagaOutboxEvent owned() {
        SagaOutboxEvent event = new SagaOutboxEvent();
        event.setStatus(SagaOutboxEvent.Status.IN_FLIGHT);
        event.setLeaseToken(token);
        event.setLeaseUntil(LocalDateTime.now().plusSeconds(30));
        when(repository.findByIdForUpdate(1L)).thenReturn(Optional.of(event));
        return event;
    }
}
