package com.delivery.saga_orchestrator_service.service;

import com.delivery.saga_orchestrator_service.entity.SagaEarlyEvent;
import com.delivery.saga_orchestrator_service.entity.SagaInstance;
import com.delivery.saga_orchestrator_service.repository.SagaEarlyEventRepository;
import com.delivery.saga_orchestrator_service.repository.SagaInboundReceiptRepository;
import com.delivery.saga_orchestrator_service.repository.SagaInstanceRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class SagaEarlyEventTest {
    private static final String CREATED = "{\"eventId\":\"11111111-1111-1111-1111-111111111111\",\"orderId\":7}";
    private static final String CANCELLED = "{\"eventId\":\"22222222-2222-2222-2222-222222222222\",\"orderId\":7}";
    private static final String CONFIRMED = "{\"eventId\":\"33333333-3333-3333-3333-333333333333\",\"orderId\":7}";

    @Test
    void cancellationBeforeSagaCreationIsDurablyStagedInsteadOfSentToDlt() {
        SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
        SagaOutboxService outbox = mock(SagaOutboxService.class);
        SagaInboundReceiptRepository receipts = mock(SagaInboundReceiptRepository.class);
        SagaEarlyEventRepository earlyEvents = mock(SagaEarlyEventRepository.class);
        when(sagas.findByOrderIdForUpdate(7L)).thenReturn(Optional.empty());
        when(earlyEvents.findById(UUID.fromString("22222222-2222-2222-2222-222222222222")))
                .thenReturn(Optional.empty());
        when(earlyEvents.insertIfAbsentPostgres(any(), any(), any(), any(), any())).thenReturn(1);

        manager(sagas, outbox, receipts, earlyEvents).handleOrderCancelled(7L, CANCELLED);

        verify(earlyEvents).insertIfAbsentPostgres(
                UUID.fromString("22222222-2222-2222-2222-222222222222"),
                "order.cancelled", 7L, CANCELLED, sha256(CANCELLED));
        verifyNoInteractions(outbox, receipts);
    }

    @Test
    void earlyCancellationIsAppliedBeforeCreateDeliveryIsDispatched() {
        SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
        SagaOutboxService outbox = mock(SagaOutboxService.class);
        SagaInboundReceiptRepository receipts = mock(SagaInboundReceiptRepository.class);
        SagaEarlyEventRepository earlyEvents = mock(SagaEarlyEventRepository.class);
        SagaEarlyEvent cancellation = new SagaEarlyEvent(
                UUID.fromString("22222222-2222-2222-2222-222222222222"),
                "order.cancelled", 7L, CANCELLED, "fingerprint");
        when(sagas.findByOrderIdForUpdate(7L)).thenReturn(Optional.empty());
        when(earlyEvents.findByOrderIdForUpdate(7L)).thenReturn(List.of(cancellation));
        when(receipts.findById(any())).thenReturn(Optional.empty());
        when(receipts.insertIfAbsentPostgres(any(), any(), any(), any())).thenReturn(1);

        manager(sagas, outbox, receipts, earlyEvents).handleOrderCreated(7L, CREATED);

        ArgumentCaptor<SagaInstance> saga = ArgumentCaptor.forClass(SagaInstance.class);
        verify(sagas, atLeastOnce()).save(saga.capture());
        assertThat(saga.getValue().getStatus()).isEqualTo(SagaInstance.SagaStatus.CANCELLED);
        assertThat(saga.getValue().getSteps()).anySatisfy(step ->
                assertThat(step.getStepName()).isEqualTo("ORDER_CANCELLED"));
        verify(earlyEvents).delete(cancellation);
        verify(outbox, never()).saveCommand(eq("7"), eq(SagaManager.CMD_CREATE_DELIVERY), eq("7"), any());
        verify(outbox).saveCommand(eq("7"), eq(SagaManager.CMD_CANCEL_DELIVERY), eq("7"), any());
        verify(outbox, never()).saveCommand(eq("7"), eq(SagaManager.CMD_STOP_MATCHING), eq("7"), any());
    }

    @Test
    void earlyRestaurantConfirmationIsAppliedThenCreateDeliveryContinuesNormally() {
        SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
        SagaOutboxService outbox = mock(SagaOutboxService.class);
        SagaInboundReceiptRepository receipts = mock(SagaInboundReceiptRepository.class);
        SagaEarlyEventRepository earlyEvents = mock(SagaEarlyEventRepository.class);
        SagaEarlyEvent confirmation = new SagaEarlyEvent(
                UUID.fromString("33333333-3333-3333-3333-333333333333"),
                "restaurant.order-confirmed", 7L, CONFIRMED, "fingerprint");
        when(sagas.findByOrderIdForUpdate(7L)).thenReturn(Optional.empty());
        when(earlyEvents.findByOrderIdForUpdate(7L)).thenReturn(List.of(confirmation));
        when(receipts.findById(any())).thenReturn(Optional.empty());
        when(receipts.insertIfAbsentPostgres(any(), any(), any(), any())).thenReturn(1);

        manager(sagas, outbox, receipts, earlyEvents).handleOrderCreated(7L, CREATED);

        ArgumentCaptor<SagaInstance> saga = ArgumentCaptor.forClass(SagaInstance.class);
        verify(sagas, atLeastOnce()).save(saga.capture());
        assertThat(saga.getValue().getStatus()).isEqualTo(SagaInstance.SagaStatus.STARTED);
        assertThat(saga.getValue().getSteps()).anySatisfy(step ->
                assertThat(step.getStepName()).isEqualTo("RESTAURANT_CONFIRMED"));
        verify(earlyEvents).delete(confirmation);
        verify(outbox).saveCommand(eq("7"), eq(SagaManager.CMD_CREATE_DELIVERY), eq("7"), any());
    }

    @Test
    void drainerPromotesAnEventThatCommittedAfterInitialSagaCreationCheck() {
        SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
        SagaOutboxService outbox = mock(SagaOutboxService.class);
        SagaInboundReceiptRepository receipts = mock(SagaInboundReceiptRepository.class);
        SagaEarlyEventRepository earlyEvents = mock(SagaEarlyEventRepository.class);
        SagaInstance saga = new SagaInstance();
        saga.setOrderId(7L);
        saga.setSagaType("ORDER_CREATION");
        saga.setStatus(SagaInstance.SagaStatus.STARTED);
        SagaEarlyEvent cancellation = new SagaEarlyEvent(
                UUID.fromString("22222222-2222-2222-2222-222222222222"),
                "order.cancelled", 7L, CANCELLED, "fingerprint");
        when(earlyEvents.findById(cancellation.getEventId())).thenReturn(Optional.of(cancellation));
        when(earlyEvents.findByIdForUpdate(cancellation.getEventId())).thenReturn(Optional.of(cancellation));
        when(sagas.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));
        when(receipts.findById(any())).thenReturn(Optional.empty());
        when(receipts.insertIfAbsentPostgres(any(), any(), any(), any())).thenReturn(1);

        manager(sagas, outbox, receipts, earlyEvents).processEarlyEvent(cancellation.getEventId());

        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.CANCELLED);
        verify(earlyEvents).delete(cancellation);
        verify(outbox, never()).saveCommand(eq("7"), eq(SagaManager.CMD_CREATE_DELIVERY), eq("7"), any());
    }

    private SagaManager manager(
            SagaInstanceRepository sagas,
            SagaOutboxService outbox,
            SagaInboundReceiptRepository receipts,
            SagaEarlyEventRepository earlyEvents) {
        return new SagaManager(sagas, outbox, receipts, earlyEvents);
    }

    private static String sha256(String value) {
        try {
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return java.util.HexFormat.of().formatHex(digest);
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
