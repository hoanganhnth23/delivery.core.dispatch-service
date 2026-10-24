package com.delivery.saga_orchestrator_service.service;

import com.delivery.saga_orchestrator_service.entity.SagaInstance;
import com.delivery.saga_orchestrator_service.repository.SagaInstanceRepository;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Optional;
import java.util.UUID;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SagaManagerOfferTimeoutTest {

    @Mock SagaInstanceRepository repository;
    @Mock SagaOutboxService outboxService;

    private static final UUID EXPIRE_COMMAND = UUID.fromString("77777777-7777-7777-7777-777777777777");

    private SagaInstance timedOutSaga() {
        SagaInstance saga = new SagaInstance();
        saga.setOrderId(10L);
        saga.setDeliveryId(20L);
        saga.setSagaType("ORDER_CREATION");
        saga.setStatus(SagaInstance.SagaStatus.SHIPPER_FOUND);
        saga.setVersion(1L);
        saga.setUpdatedAt(LocalDateTime.now().minusMinutes(5));
        saga.setPayload("""
                {"orderId":10,"totalPrice":120000,"shippingFee":20000,
                 "paymentMethod":"COD","restaurantId":40,"restaurantName":"Test"}
                """);
        saga.addStep("DELIVERY_CREATED", "delivery.created.result", """
                {"orderId":10,"deliveryId":20,"pickupLat":10.75,"pickupLng":106.67,
                 "deliveryLat":10.76,"deliveryLng":106.68}
                """);
        saga.addStep("SHIPPER_FOUND", "shipper.found", """
                {"orderId":10,"deliveryId":20,"pickupLat":10.75,"pickupLng":106.67,
                 "foundAt":"2026-07-25T13:00:00","waitingTimeoutSeconds":180,
                 "availableShippers":[{"shipperId":30}]}
                """);
        return saga;
    }

    private static String retired(String outcome, UUID source, Long shipperId) {
        return "{\"eventId\":\"" + UUID.randomUUID() + "\",\"sourceCommandEventId\":\"" + source
                + "\",\"orderId\":10,\"deliveryId\":20,\"outcome\":\"" + outcome + "\""
                + (shipperId == null ? "" : ",\"shipperId\":" + shipperId) + "}";
    }

    @Test
    void timedOutOfferWaitsForDeliveryRetirementBeforeRematching() {
        SagaInstance saga = timedOutSaga();
        when(repository.findByOrderIdForUpdate(10L)).thenReturn(Optional.of(saga));
        when(outboxService.saveCommand(eq("10"), eq(SagaManager.CMD_EXPIRE_SHIPPER_OFFER), eq("10"), any()))
                .thenReturn(EXPIRE_COMMAND);
        new SagaManager(repository, outboxService).handleShipperOfferTimeout(10L);

        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.OFFER_RETIRING);
        assertThat(saga.getSteps()).anyMatch(step -> step.getStepName().startsWith("SHIPPER_OFFER_TIMEOUT_"));
        verify(repository).save(saga);
        verify(outboxService, never()).saveCommand(anyString(), eq(SagaManager.CMD_FIND_SHIPPER), anyString(), any());
        verify(outboxService, never()).saveCommand(anyString(), eq(SagaManager.CMD_UPDATE_ORDER_STATUS), anyString(), any());

        ArgumentCaptor<Object> expirePayload = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).saveCommand(eq("10"), eq(SagaManager.CMD_EXPIRE_SHIPPER_OFFER),
                eq("10"), expirePayload.capture());
        JsonNode expire = (JsonNode) expirePayload.getValue();
        assertThat(expire.get("deliveryId").asLong()).isEqualTo(20L);
        assertThat(expire.get("timedOutShipperId").asLong()).isEqualTo(30L);
        assertThat(java.time.LocalDateTime.parse(expire.get("expectedOfferExpiresAt").asText()))
                .isEqualTo(java.time.LocalDateTime.of(2026, 7, 25, 13, 3));
    }

    @Test
    void retiredOfferStartsThePreparedRematchExcludingThePreviousShipper() {
        SagaInstance saga = timedOutSaga();
        when(repository.findByOrderIdForUpdate(10L)).thenReturn(Optional.of(saga));
        when(outboxService.saveCommand(eq("10"), eq(SagaManager.CMD_EXPIRE_SHIPPER_OFFER), eq("10"), any()))
                .thenReturn(EXPIRE_COMMAND);
        SagaManager manager = new SagaManager(repository, outboxService);
        manager.handleShipperOfferTimeout(10L);

        manager.handleOfferRetired(10L, 20L, retired("RETIRED", EXPIRE_COMMAND, null));

        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.FINDING_SHIPPER);
        ArgumentCaptor<Object> payload = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).saveCommand(eq("10"), eq(SagaManager.CMD_FIND_SHIPPER),
                eq("10"), payload.capture());
        JsonNode command = (JsonNode) payload.getValue();
        assertThat(command.get("excludedShipperIds").get(0).asLong()).isEqualTo(30L);
        assertThat(command.get("totalPrice").decimalValue()).isEqualByComparingTo("120000");
        assertThat(command.get("paymentMethod").asText()).isEqualTo("COD");
        assertThat(command.get("deliveryLat").asDouble()).isEqualTo(10.76);
        assertThat(command.hasNonNull("matchingSessionId")).isTrue();
        ArgumentCaptor<Object> status = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).saveCommand(eq("10"), eq(SagaManager.CMD_UPDATE_ORDER_STATUS),
                eq("10"), status.capture());
        assertThat(((JsonNode) status.getValue()).get("sagaStatus").asText()).isEqualTo("FINDING_SHIPPER");
    }

    @Test
    void retirementReportingCommittedAcceptanceConvergesToAssigned() {
        SagaInstance saga = timedOutSaga();
        when(repository.findByOrderIdForUpdate(10L)).thenReturn(Optional.of(saga));
        when(outboxService.saveCommand(eq("10"), eq(SagaManager.CMD_EXPIRE_SHIPPER_OFFER), eq("10"), any()))
                .thenReturn(EXPIRE_COMMAND);
        SagaManager manager = new SagaManager(repository, outboxService);
        manager.handleShipperOfferTimeout(10L);

        manager.handleOfferRetired(10L, 20L, retired("ASSIGNED", EXPIRE_COMMAND, 30L));

        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.SHIPPER_ASSIGNED);
        assertThat(saga.getShipperId()).isEqualTo(30L);
        verify(outboxService, never()).saveCommand(anyString(), eq(SagaManager.CMD_FIND_SHIPPER), anyString(), any());
        // The separately published acceptance is then an exact replay.
        clearInvocations(outboxService);
        manager.handleShipperAccepted(10L, 20L, 30L, "{\"orderId\":10,\"deliveryId\":20,\"shipperId\":30}");
        verifyNoInteractions(outboxService);
    }

    @Test
    void acceptanceDuringRetirementConvergesAndLateRetirementIsIgnored() {
        SagaInstance saga = timedOutSaga();
        when(repository.findByOrderIdForUpdate(10L)).thenReturn(Optional.of(saga));
        when(outboxService.saveCommand(eq("10"), eq(SagaManager.CMD_EXPIRE_SHIPPER_OFFER), eq("10"), any()))
                .thenReturn(EXPIRE_COMMAND);
        SagaManager manager = new SagaManager(repository, outboxService);
        manager.handleShipperOfferTimeout(10L);

        manager.handleShipperAccepted(10L, 20L, 30L, "{\"orderId\":10,\"deliveryId\":20,\"shipperId\":30}");
        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.SHIPPER_ASSIGNED);
        clearInvocations(outboxService);

        manager.handleOfferRetired(10L, 20L, retired("ASSIGNED", EXPIRE_COMMAND, 30L));
        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.SHIPPER_ASSIGNED);
        verifyNoInteractions(outboxService);
    }

    @Test
    void retirementForAnotherExpireCommandIsIgnored() {
        SagaInstance saga = timedOutSaga();
        when(repository.findByOrderIdForUpdate(10L)).thenReturn(Optional.of(saga));
        when(outboxService.saveCommand(eq("10"), eq(SagaManager.CMD_EXPIRE_SHIPPER_OFFER), eq("10"), any()))
                .thenReturn(EXPIRE_COMMAND);
        SagaManager manager = new SagaManager(repository, outboxService);
        manager.handleShipperOfferTimeout(10L);
        clearInvocations(outboxService);

        manager.handleOfferRetired(10L, 20L, retired("RETIRED", UUID.randomUUID(), null));
        manager.handleOfferRetired(10L, 20L, retired("TERMINAL", EXPIRE_COMMAND, null));

        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.OFFER_RETIRING);
        verifyNoInteractions(outboxService);
    }

    @Test
    void timeoutCommandsUseRecoveryTopicOverrides() {
        SagaInstance saga = timedOutSaga();
        when(repository.findByOrderIdForUpdate(10L)).thenReturn(Optional.of(saga));
        when(outboxService.saveCommand(eq("10"), eq("b8.delivery.expire"), eq("10"), any()))
                .thenReturn(EXPIRE_COMMAND);

        SagaManager manager = new SagaManager(repository, outboxService);
        ReflectionTestUtils.setField(manager, "expireShipperOfferTopic", "b8.delivery.expire");
        ReflectionTestUtils.setField(manager, "findShipperTopic", "b8.match.find");
        ReflectionTestUtils.setField(manager, "updateOrderStatusTopic", "b8.order.status");
        manager.handleShipperOfferTimeout(10L);
        manager.handleOfferRetired(10L, 20L, retired("RETIRED", EXPIRE_COMMAND, null));

        verify(outboxService).saveCommand(eq("10"), eq("b8.delivery.expire"), eq("10"), any());
        verify(outboxService).saveCommand(eq("10"), eq("b8.match.find"), eq("10"), any());
        verify(outboxService).saveCommand(eq("10"), eq("b8.order.status"), eq("10"), any());
    }

    @Test
    void timeoutPollBeforeTheOfferDeadlineIsANoOp() {
        SagaInstance saga = offerSaga(LocalDateTime.now().plusMinutes(1), 120, 30L);
        when(repository.findByOrderIdForUpdate(10L)).thenReturn(Optional.of(saga));

        new SagaManager(repository, outboxService).handleShipperOfferTimeout(10L);

        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.SHIPPER_FOUND);
        assertThat(saga.getSteps()).noneMatch(step ->
                step.getStepName().startsWith("SHIPPER_OFFER_TIMEOUT_"));
        verify(repository, never()).save(any());
        verifyNoInteractions(outboxService);
    }

    @Test
    void malformedShipperIdentityFailsClosedWithoutQueuingRematch() {
        SagaInstance saga = offerSaga(LocalDateTime.now().minusMinutes(5), 60, 0L);
        when(repository.findByOrderIdForUpdate(10L)).thenReturn(Optional.of(saga));

        new SagaManager(repository, outboxService).handleShipperOfferTimeout(10L);

        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.FAILED);
        verify(outboxService, never()).saveCommand(eq("10"),
                eq(SagaManager.CMD_EXPIRE_SHIPPER_OFFER), eq("10"), any());
        verify(outboxService, never()).saveCommand(eq("10"),
                eq(SagaManager.CMD_FIND_SHIPPER), eq("10"), any());
        verify(outboxService).saveCommand(eq("10"),
                eq(SagaManager.CMD_MARK_SHIPPER_NOT_FOUND), eq("10"), any());
    }

    private SagaInstance offerSaga(LocalDateTime foundAt, int timeoutSeconds, long shipperId) {
        SagaInstance saga = new SagaInstance();
        saga.setOrderId(10L);
        saga.setDeliveryId(20L);
        saga.setSagaType("ORDER_CREATION");
        saga.setStatus(SagaInstance.SagaStatus.SHIPPER_FOUND);
        saga.setVersion(1L);
        saga.setUpdatedAt(LocalDateTime.now().minusMinutes(5));
        saga.setPayload("{\"orderId\":10,\"totalPrice\":120000,\"shippingFee\":20000,"
                + "\"paymentMethod\":\"COD\",\"restaurantId\":40}");
        saga.addStep("DELIVERY_CREATED", "delivery.created.result",
                "{\"orderId\":10,\"deliveryId\":20}");
        saga.addStep("SHIPPER_FOUND", "shipper.found",
                "{\"orderId\":10,\"deliveryId\":20,\"foundAt\":\"" + foundAt
                        + "\",\"waitingTimeoutSeconds\":" + timeoutSeconds
                        + ",\"availableShippers\":[{\"shipperId\":" + shipperId + "}]}");
        return saga;
    }
}
