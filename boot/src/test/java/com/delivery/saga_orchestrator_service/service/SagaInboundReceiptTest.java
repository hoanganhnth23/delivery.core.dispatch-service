package com.delivery.saga_orchestrator_service.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.delivery.saga_orchestrator_service.entity.SagaInboundReceipt;
import com.delivery.saga_orchestrator_service.entity.SagaInstance;
import com.delivery.saga_orchestrator_service.repository.SagaInboundReceiptRepository;
import com.delivery.saga_orchestrator_service.repository.SagaInstanceRepository;
import com.fasterxml.jackson.databind.ObjectMapper;

class SagaInboundReceiptTest {
    private static final UUID EVENT_ID = UUID.fromString("88888888-8888-8888-8888-888888888888");
    private static final String EVENT = "{\"eventId\":\"" + EVENT_ID + "\",\"orderId\":91}";

    @Test
    void exactInboundReplayDoesNotReachSagaMutationOrOutbox() throws Exception {
        SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
        SagaOutboxService outbox = mock(SagaOutboxService.class);
        SagaInboundReceiptRepository receipts = mock(SagaInboundReceiptRepository.class);
        when(receipts.findById(EVENT_ID)).thenReturn(Optional.of(new SagaInboundReceipt(
                EVENT_ID, "order.created", 91L, sha256(EVENT))));

        SagaManager manager = new SagaManager(sagas, outbox, receipts);
        assertDoesNotThrow(() -> manager.handleOrderCreated(91L, EVENT));
        verifyNoInteractions(sagas, outbox);
    }

    @Test
    void conflictingPayloadForStableEventIdIsRejectedBeforeSagaMutation() throws Exception {
        SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
        SagaOutboxService outbox = mock(SagaOutboxService.class);
        SagaInboundReceiptRepository receipts = mock(SagaInboundReceiptRepository.class);
        when(receipts.findById(EVENT_ID)).thenReturn(Optional.of(new SagaInboundReceipt(
                EVENT_ID, "order.created", 91L, sha256(EVENT))));

        SagaManager manager = new SagaManager(sagas, outbox, receipts);
        assertThrows(IllegalArgumentException.class, () -> manager.handleOrderCreated(91L,
                "{\"eventId\":\"" + EVENT_ID + "\",\"orderId\":92}"));
        verifyNoInteractions(sagas, outbox);
    }

    @Test
    void timeoutUsesDurableInboxIdentityBeforeCompensation() throws Exception {
        SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
        SagaOutboxService outbox = mock(SagaOutboxService.class);
        SagaInboundReceiptRepository receipts = mock(SagaInboundReceiptRepository.class);
        SagaInstance saga = timeoutCandidate(91L, SagaInstance.SagaStatus.FINDING_SHIPPER, 4L,
                LocalDateTime.now().minusMinutes(10));
        SagaTimeoutCommand timeout = SagaTimeoutCommand.forStage(
                saga, SagaInstance.SagaStatus.FINDING_SHIPPER, Duration.ofMinutes(5), "matching timeout");
        when(sagas.findByOrderIdForUpdate(91L)).thenReturn(Optional.of(saga));
        when(receipts.findById(timeout.eventId())).thenReturn(Optional.empty());
        when(receipts.insertIfAbsentPostgres(any(), any(), any(), any())).thenReturn(1);

        new SagaManager(sagas, outbox, receipts).handleTimeout(timeout);

        verify(receipts).insertIfAbsentPostgres(timeout.eventId(), "saga.timeout.FINDING_SHIPPER",
                91L, sha256(timeout.toJson(new ObjectMapper())));
        org.mockito.Mockito.verify(outbox).saveCommand(org.mockito.ArgumentMatchers.eq("91"),
                org.mockito.ArgumentMatchers.eq(SagaManager.CMD_MARK_SHIPPER_NOT_FOUND),
                org.mockito.ArgumentMatchers.eq("91"), org.mockito.ArgumentMatchers.any());
    }

    @Test
    void staleTimeoutDoesNotClaimInboxOrCompensateANewerSagaVersion() {
        SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
        SagaOutboxService outbox = mock(SagaOutboxService.class);
        SagaInboundReceiptRepository receipts = mock(SagaInboundReceiptRepository.class);
        SagaInstance observed = timeoutCandidate(91L, SagaInstance.SagaStatus.FINDING_SHIPPER, 4L,
                LocalDateTime.now().minusMinutes(10));
        SagaTimeoutCommand timeout = SagaTimeoutCommand.forStage(
                observed, SagaInstance.SagaStatus.FINDING_SHIPPER, Duration.ofMinutes(5), "matching timeout");
        SagaInstance current = timeoutCandidate(91L, SagaInstance.SagaStatus.SHIPPER_FOUND, 5L,
                LocalDateTime.now());
        when(sagas.findByOrderIdForUpdate(91L)).thenReturn(Optional.of(current));

        new SagaManager(sagas, outbox, receipts).handleTimeout(timeout);

        verifyNoInteractions(receipts, outbox);
        org.mockito.Mockito.verify(sagas, org.mockito.Mockito.never()).save(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void earlyTimeoutDoesNotClaimInboxOrCompensate() {
        SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
        SagaOutboxService outbox = mock(SagaOutboxService.class);
        SagaInboundReceiptRepository receipts = mock(SagaInboundReceiptRepository.class);
        SagaInstance saga = timeoutCandidate(91L, SagaInstance.SagaStatus.FINDING_SHIPPER, 4L,
                LocalDateTime.now());
        SagaTimeoutCommand timeout = SagaTimeoutCommand.forStage(
                saga, SagaInstance.SagaStatus.FINDING_SHIPPER, Duration.ofMinutes(5), "matching timeout");
        when(sagas.findByOrderIdForUpdate(91L)).thenReturn(Optional.of(saga));

        new SagaManager(sagas, outbox, receipts).handleTimeout(timeout);

        verifyNoInteractions(receipts, outbox);
        org.mockito.Mockito.verify(sagas, org.mockito.Mockito.never()).save(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void exactTimeoutReplaySkipsMutationAfterItsReceiptExists() throws Exception {
        SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
        SagaOutboxService outbox = mock(SagaOutboxService.class);
        SagaInboundReceiptRepository receipts = mock(SagaInboundReceiptRepository.class);
        SagaInstance saga = timeoutCandidate(91L, SagaInstance.SagaStatus.FINDING_SHIPPER, 4L,
                LocalDateTime.now().minusMinutes(10));
        SagaTimeoutCommand timeout = SagaTimeoutCommand.forStage(
                saga, SagaInstance.SagaStatus.FINDING_SHIPPER, Duration.ofMinutes(5), "matching timeout");
        String raw = timeout.toJson(new ObjectMapper());
        when(sagas.findByOrderIdForUpdate(91L)).thenReturn(Optional.of(saga));
        when(receipts.findById(timeout.eventId())).thenReturn(Optional.of(new SagaInboundReceipt(
                timeout.eventId(), "saga.timeout.FINDING_SHIPPER", 91L, sha256(raw))));

        new SagaManager(sagas, outbox, receipts).handleTimeout(timeout);

        verifyNoInteractions(outbox);
        org.mockito.Mockito.verify(sagas, org.mockito.Mockito.never()).save(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void atomicClaimConflictLoadsCommittedExactReceiptAndSkipsMutation() throws Exception {
        SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
        SagaOutboxService outbox = mock(SagaOutboxService.class);
        SagaInboundReceiptRepository receipts = mock(SagaInboundReceiptRepository.class);
        SagaInboundReceipt committed = new SagaInboundReceipt(EVENT_ID, "order.created", 91L, sha256(EVENT));
        when(receipts.findById(EVENT_ID)).thenReturn(Optional.empty(), Optional.of(committed));
        when(receipts.insertIfAbsentPostgres(EVENT_ID, "order.created", 91L, sha256(EVENT))).thenReturn(0);

        new SagaManager(sagas, outbox, receipts).handleOrderCreated(91L, EVENT);

        verify(receipts).insertIfAbsentPostgres(EVENT_ID, "order.created", 91L, sha256(EVENT));
        verifyNoInteractions(sagas, outbox);
    }

    private static SagaInstance timeoutCandidate(
            Long orderId,
            SagaInstance.SagaStatus status,
            Long version,
            LocalDateTime updatedAt) {
        SagaInstance saga = new SagaInstance();
        saga.setOrderId(orderId);
        saga.setDeliveryId(92L);
        saga.setSagaType("ORDER_CREATION");
        saga.setStatus(status);
        saga.setVersion(version);
        saga.setUpdatedAt(updatedAt);
        return saga;
    }

    private static String sha256(String value) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        StringBuilder result = new StringBuilder();
        for (byte item : digest) result.append(String.format("%02x", item));
        return result.toString();
    }
}
