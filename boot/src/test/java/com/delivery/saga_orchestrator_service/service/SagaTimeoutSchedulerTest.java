package com.delivery.saga_orchestrator_service.service;

import com.delivery.saga_orchestrator_service.entity.SagaInstance;
import com.delivery.saga_orchestrator_service.repository.SagaInstanceRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.domain.Pageable;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SagaTimeoutSchedulerTest {

    @Test
    void boundsTimeoutQueryAndContinuesAfterOneSagaFails() {
        SagaInstanceRepository repository = mock(SagaInstanceRepository.class);
        SagaManager manager = mock(SagaManager.class);
        SagaInstance first = stuckSaga(1L, 11L, SagaInstance.SagaStatus.FINDING_SHIPPER);
        SagaInstance second = stuckSaga(2L, 22L, SagaInstance.SagaStatus.FINDING_SHIPPER);
        when(repository.findStuckSagas(eq(SagaInstance.SagaStatus.FINDING_SHIPPER),
                any(LocalDateTime.class), any(Pageable.class)))
                .thenReturn(List.of(first, second));
        doThrow(new IllegalStateException("poison saga"))
                .when(manager).handleTimeout(argThat(command -> command.orderId().equals(1L)));
        SagaTimeoutScheduler scheduler = new SagaTimeoutScheduler(repository, manager, 25);

        scheduler.checkTimeouts();

        ArgumentCaptor<Pageable> page = ArgumentCaptor.forClass(Pageable.class);
        verify(repository).findStuckSagas(eq(SagaInstance.SagaStatus.FINDING_SHIPPER),
                any(LocalDateTime.class), page.capture());
        assertThat(page.getValue().getPageNumber()).isZero();
        assertThat(page.getValue().getPageSize()).isEqualTo(25);
        ArgumentCaptor<SagaTimeoutCommand> timeout = ArgumentCaptor.forClass(SagaTimeoutCommand.class);
        verify(manager, times(2)).handleTimeout(timeout.capture());
        assertThat(timeout.getAllValues()).anySatisfy(command -> {
            assertThat(command.orderId()).isEqualTo(2L);
            assertThat(command.deliveryId()).isEqualTo(22L);
            assertThat(command.expectedStatus()).isEqualTo(SagaInstance.SagaStatus.FINDING_SHIPPER);
            assertThat(command.eventId()).isNotNull();
            assertThat(command.expectedVersion()).isEqualTo(4L);
        });
    }

    @Test
    void scansShipperOffersFromMinimumContractAgeInsteadOfFixedThreeMinutes() {
        SagaInstanceRepository repository = mock(SagaInstanceRepository.class);
        SagaManager manager = mock(SagaManager.class);
        SagaInstance candidate = stuckSaga(3L, 33L, SagaInstance.SagaStatus.SHIPPER_FOUND);
        when(repository.findStuckSagas(eq(SagaInstance.SagaStatus.SHIPPER_FOUND),
                any(LocalDateTime.class), any(Pageable.class)))
                .thenReturn(List.of(candidate));
        SagaTimeoutScheduler scheduler = new SagaTimeoutScheduler(repository, manager, 25);
        LocalDateTime beforePoll = LocalDateTime.now();

        scheduler.checkTimeouts();

        ArgumentCaptor<LocalDateTime> cutoff = ArgumentCaptor.forClass(LocalDateTime.class);
        verify(repository).findStuckSagas(eq(SagaInstance.SagaStatus.SHIPPER_FOUND),
                cutoff.capture(), any(Pageable.class));
        assertThat(cutoff.getValue()).isAfter(beforePoll.minusSeconds(5));
        ArgumentCaptor<SagaTimeoutCommand> timeout = ArgumentCaptor.forClass(SagaTimeoutCommand.class);
        verify(manager).handleTimeout(timeout.capture());
        assertThat(timeout.getAllValues()).anySatisfy(command -> {
            assertThat(command.orderId()).isEqualTo(3L);
            assertThat(command.expectedStatus()).isEqualTo(SagaInstance.SagaStatus.SHIPPER_FOUND);
            assertThat(command.eventId()).isNotNull();
        });
    }

    private SagaInstance stuckSaga(Long orderId, Long deliveryId, SagaInstance.SagaStatus status) {
        SagaInstance saga = new SagaInstance();
        saga.setOrderId(orderId);
        saga.setDeliveryId(deliveryId);
        saga.setStatus(status);
        saga.setVersion(4L);
        saga.setUpdatedAt(LocalDateTime.now().minusMinutes(10));
        return saga;
    }
}
