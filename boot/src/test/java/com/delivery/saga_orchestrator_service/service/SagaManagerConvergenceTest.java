package com.delivery.saga_orchestrator_service.service;

import com.delivery.saga_orchestrator_service.entity.SagaInstance;
import com.delivery.saga_orchestrator_service.repository.SagaInstanceRepository;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SagaManagerConvergenceTest {

    private SagaInstanceRepository repository;
    private SagaOutboxService outboxService;
    private SagaManager manager;

    @BeforeEach
    void setUp() {
        repository = mock(SagaInstanceRepository.class);
        outboxService = mock(SagaOutboxService.class);
        manager = new SagaManager(repository, outboxService);
    }

    @Test
    void rejectsOutOfOrderDeliveryCompletion() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.SHIPPER_ASSIGNED);
        saga.setDeliveryId(8L);
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        assertThrows(IllegalStateException.class,
                () -> manager.handleDeliveryStatusUpdated(
                        7L, 8L, "DELIVERED", "{\"orderId\":7,\"deliveryId\":8}"));

        verify(repository, never()).save(any());
        verifyNoInteractions(outboxService);
    }

    @Test
    void exactOldDeliveryStatusReplayIsSkippedAfterSagaAdvanced() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.COMPLETED);
        saga.setDeliveryId(8L);
        String event = "{\"eventId\":\"11111111-1111-1111-1111-111111111111\","
                + "\"orderId\":7,\"deliveryId\":8,\"status\":\"PICKED_UP\"}";
        saga.addStep("DELIVERY_PICKED_UP", "delivery.status-updated", event);
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        manager.handleDeliveryStatusUpdated(7L, 8L, "PICKED_UP", event);

        verify(repository, never()).save(any());
        verifyNoInteractions(outboxService);
    }

    @Test
    void sameTerminalStatusWithDifferentEventIsRejected() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.COMPLETED);
        saga.setDeliveryId(8L);
        saga.addStep("DELIVERY_DELIVERED", "delivery.status-updated",
                "{\"eventId\":\"11111111-1111-1111-1111-111111111111\","
                        + "\"orderId\":7,\"deliveryId\":8,\"status\":\"DELIVERED\"}");
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        assertThrows(IllegalStateException.class,
                () -> manager.handleDeliveryStatusUpdated(7L, 8L, "DELIVERED",
                        "{\"eventId\":\"22222222-2222-2222-2222-222222222222\","
                                + "\"orderId\":7,\"deliveryId\":8,\"status\":\"DELIVERED\"}"));

        verify(repository, never()).save(any());
        verifyNoInteractions(outboxService);
    }

    @Test
    void contradictoryDeliveryStatusAfterCompletedSagaIsRejected() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.COMPLETED);
        saga.setDeliveryId(8L);
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        assertThrows(IllegalStateException.class,
                () -> manager.handleDeliveryStatusUpdated(7L, 8L, "CANCELLED",
                        "{\"eventId\":\"33333333-3333-3333-3333-333333333333\","
                                + "\"orderId\":7,\"deliveryId\":8,\"status\":\"CANCELLED\"}"));

        verify(repository, never()).save(any());
        verifyNoInteractions(outboxService);
    }

    @Test
    void matchingTimeoutConvergesOrderToShipperNotFound() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.FINDING_SHIPPER);
        saga.setDeliveryId(8L);
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        manager.handleStepFailed("TIMEOUT_FINDING_SHIPPER", 7L, "timeout",
                "{\"orderId\":7,\"deliveryId\":8}");

        ArgumentCaptor<Object> payload = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).saveCommand(eq("7"), eq(SagaManager.CMD_UPDATE_ORDER_STATUS),
                eq("7"), payload.capture());
        assertThat(((JsonNode) payload.getValue()).get("sagaStatus").asText())
                .isEqualTo("SHIPPER_NOT_FOUND");
        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.FAILED);
        verify(outboxService).saveCommand(eq("7"),
                eq(SagaManager.CMD_MARK_SHIPPER_NOT_FOUND), eq("7"), any());
        verify(outboxService, never()).saveCommand(eq("7"),
                eq(SagaManager.CMD_CANCEL_DELIVERY), eq("7"), any());
    }

    @Test
    void exhaustedMatchingMarksDeliveryAndOrderShipperNotFound() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.FINDING_SHIPPER);
        saga.setDeliveryId(8L);
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));
        String event = """
                {"eventId":"11111111-1111-1111-1111-111111111111",
                 "orderId":7,"deliveryId":8,"retryAttempts":10}
                """;

        manager.handleShipperNotFound(7L, 8L, event);

        verify(outboxService).saveCommand(eq("7"),
                eq(SagaManager.CMD_MARK_SHIPPER_NOT_FOUND), eq("7"), any());
        verify(outboxService, never()).saveCommand(eq("7"),
                eq(SagaManager.CMD_CANCEL_DELIVERY), eq("7"), any());
        ArgumentCaptor<Object> status = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).saveCommand(eq("7"),
                eq(SagaManager.CMD_UPDATE_ORDER_STATUS), eq("7"), status.capture());
        assertThat(((JsonNode) status.getValue()).get("sagaStatus").asText())
                .isEqualTo("SHIPPER_NOT_FOUND");
    }

    @Test
    void deliveryShipperNotFoundStatusEchoDoesNotIssueDuplicateOrderCommand() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.FAILED);
        saga.setDeliveryId(8L);
        saga.addStep("SHIPPER_NOT_FOUND", "shipper.not-found",
                "{\"eventId\":\"11111111-1111-1111-1111-111111111111\","
                        + "\"orderId\":7,\"deliveryId\":8}");
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));
        String event = "{\"eventId\":\"22222222-2222-2222-2222-222222222222\","
                + "\"orderId\":7,\"deliveryId\":8,\"status\":\"SHIPPER_NOT_FOUND\"}";

        manager.handleDeliveryStatusUpdated(7L, 8L, "SHIPPER_NOT_FOUND", event);

        assertThat(saga.getSteps()).anySatisfy(step -> {
            assertThat(step.getStepName()).isEqualTo("DELIVERY_SHIPPER_NOT_FOUND");
            assertThat(step.getEventType()).isEqualTo("delivery.status-updated");
            assertThat(step.getEventData()).isEqualTo(event);
        });
        verify(repository).save(saga);
        verifyNoInteractions(outboxService);
    }

    @Test
    void deliveryShipperNotFoundStatusBeforeMatchingTerminalFailsClosed() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.FINDING_SHIPPER);
        saga.setDeliveryId(8L);
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        assertThrows(IllegalStateException.class,
                () -> manager.handleDeliveryStatusUpdated(7L, 8L, "SHIPPER_NOT_FOUND",
                        "{\"eventId\":\"22222222-2222-2222-2222-222222222222\","
                                + "\"orderId\":7,\"deliveryId\":8,\"status\":\"SHIPPER_NOT_FOUND\"}"));

        verify(repository, never()).save(any());
        verifyNoInteractions(outboxService);
    }

    @Test
    void matchingFailureEnrichesOrderCommandWithPersistedDeliveryIdentity() throws Exception {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.FINDING_SHIPPER);
        saga.setDeliveryId(8L);
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        manager.handleStepFailed("TIMEOUT_FINDING_SHIPPER", 7L, "timeout",
                "{\"orderId\":7}");

        ArgumentCaptor<Object> payload = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).saveCommand(eq("7"), eq(SagaManager.CMD_UPDATE_ORDER_STATUS),
                eq("7"), payload.capture());
        JsonNode command = (JsonNode) payload.getValue();
        JsonNode originalEvent = new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(command.get("originalEvent").asText());
        assertThat(originalEvent.get("deliveryId").asLong()).isEqualTo(8L);
    }

    @Test
    void deliveryCreationFailureConvergesOrderToCancelled() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.STARTED);
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        manager.handleDeliveryCreationFailed(7L, "cannot create", "{\"orderId\":7}");

        ArgumentCaptor<Object> payload = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).saveCommand(eq("7"), eq(SagaManager.CMD_UPDATE_ORDER_STATUS),
                eq("7"), payload.capture());
        assertThat(((JsonNode) payload.getValue()).get("sagaStatus").asText())
                .isEqualTo("CANCELLED");
    }

    @Test
    void contradictoryOrderCreatedReplayIsRejected() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.STARTED);
        saga.setPayload("{\"orderId\":7,\"eventId\":\"11111111-1111-1111-1111-111111111111\",\"totalPrice\":100000}");
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        assertThrows(IllegalStateException.class,
                () -> manager.handleOrderCreated(7L,
                        "{\"orderId\":7,\"eventId\":\"22222222-2222-2222-2222-222222222222\",\"totalPrice\":200000}"));

        verify(repository, never()).save(any());
        verifyNoInteractions(outboxService);
    }

    @Test
    void contradictoryCancellationAfterCompletedSagaIsRejected() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.COMPLETED);
        saga.setDeliveryId(8L);
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        assertThrows(IllegalStateException.class,
                () -> manager.handleOrderCancelled(7L,
                        "{\"eventId\":\"11111111-1111-1111-1111-111111111111\",\"orderId\":7}"));

        verify(repository, never()).save(any());
        verifyNoInteractions(outboxService);
    }

    @Test
    void exactDeliveryCreatedReplayIsSkipped() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.DELIVERY_CREATED);
        saga.setDeliveryId(8L);
        saga.addStep("DELIVERY_CREATED", "delivery.created.result",
                "{\"orderId\":7,\"deliveryId\":8}");
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        manager.handleDeliveryCreated(7L, 8L, "{\"orderId\":7,\"deliveryId\":8}");

        verify(repository, never()).save(any());
        verifyNoInteractions(outboxService);
    }

    @Test
    void initialMatchingAlsoMovesOrderToFindingShipper() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.DELIVERY_CREATED);
        saga.setDeliveryId(8L);
        saga.setPayload("{\"orderId\":7,\"totalPrice\":120000,\"shippingFee\":20000,"
                + "\"paymentMethod\":\"COD\",\"restaurantId\":3}");
        saga.addStep("DELIVERY_CREATED", "delivery.created.result",
                "{\"orderId\":7,\"deliveryId\":8,\"pickupLat\":10.1,\"pickupLng\":106.1,"
                        + "\"deliveryLat\":10.2,\"deliveryLng\":106.2}");
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        manager.handleRestaurantConfirmed(7L, "{\"orderId\":7,\"restaurantId\":3}");

        ArgumentCaptor<Object> matchPayload = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).saveCommand(eq("7"), eq(SagaManager.CMD_FIND_SHIPPER),
                eq("7"), matchPayload.capture());
        assertThat(((JsonNode) matchPayload.getValue()).path("matchingDeadlineAt").asText()).isNotBlank();
        ArgumentCaptor<Object> statusPayload = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).saveCommand(eq("7"), eq(SagaManager.CMD_UPDATE_ORDER_STATUS),
                eq("7"), statusPayload.capture());
        assertThat(((JsonNode) statusPayload.getValue()).get("sagaStatus").asText())
                .isEqualTo("FINDING_SHIPPER");
        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.FINDING_SHIPPER);
        verify(repository, times(2)).save(saga);
    }

    @Test
    void deliveryCreatedStateWithoutCanonicalStepFailsClosed() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.DELIVERY_CREATED);
        saga.setDeliveryId(8L);
        saga.setPayload("{\"orderId\":7,\"totalPrice\":120000,\"shippingFee\":20000,"
                + "\"paymentMethod\":\"COD\",\"restaurantId\":3}");
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        assertThrows(IllegalStateException.class,
                () -> manager.handleRestaurantConfirmed(
                        7L, "{\"orderId\":7,\"restaurantId\":3}"));

        verifyNoInteractions(outboxService);
        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.DELIVERY_CREATED);
    }

    @Test
    void lateDeliveryCreatedAfterCancellationRecordsIdentityAndReissuesCompensation() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.CANCELLED);
        saga.addStep("ORDER_CANCELLED", "order.cancelled", "{\"orderId\":7}");
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        manager.handleDeliveryCreated(7L, 8L, "{\"orderId\":7,\"deliveryId\":8}");

        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.CANCELLED);
        assertThat(saga.getDeliveryId()).isEqualTo(8L);
        assertThat(saga.getSteps()).anySatisfy(step -> {
            assertThat(step.getStepName()).isEqualTo("DELIVERY_CREATED");
            assertThat(step.getEventType()).isEqualTo("delivery.created.result");
        });
        verify(repository).save(saga);
        ArgumentCaptor<Object> payload = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).saveCommand(eq("7"), eq(SagaManager.CMD_CANCEL_DELIVERY),
                eq("7"), payload.capture());
        assertThat((JsonNode) payload.getValue()).isEqualTo(
                new com.fasterxml.jackson.databind.ObjectMapper()
                        .createObjectNode().put("orderId", 7).put("deliveryId", 8));
    }

    @Test
    void lateDeliveryCreatedAfterStartedTimeoutFailureIsCancelledInsteadOfOrphaned() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.FAILED);
        saga.addStep("TIMEOUT_STARTED_FAILED", "TIMEOUT_STARTED.failed", "{\"orderId\":7}");
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        manager.handleDeliveryCreated(7L, 8L, "{\"orderId\":7,\"deliveryId\":8}");

        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.FAILED);
        assertThat(saga.getDeliveryId()).isEqualTo(8L);
        verify(outboxService).saveCommand(eq("7"), eq(SagaManager.CMD_CANCEL_DELIVERY),
                eq("7"), any());
    }

    @Test
    void knownDeliveryCancellationWaitsForDurableDeliveryConfirmation() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.FINDING_SHIPPER);
        saga.setDeliveryId(8L);
        saga.addStep("MATCHING_STARTED", SagaManager.CMD_FIND_SHIPPER,
                "{\"orderId\":7,\"deliveryId\":8,"
                        + "\"matchingSessionId\":\"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\"}");
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        manager.handleOrderCancelled(7L, "{\"orderId\":7}");

        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.COMPENSATING);
        assertThat(saga.getCompletedAt()).isNull();
        verify(outboxService).saveCommand(eq("7"), eq(SagaManager.CMD_CANCEL_DELIVERY), eq("7"), any());
        ArgumentCaptor<Object> stopPayload = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).saveCommand(eq("7"), eq(SagaManager.CMD_STOP_MATCHING),
                eq("7"), stopPayload.capture());
        assertThat(((JsonNode) stopPayload.getValue()).get("matchingSessionId").asText())
                .isEqualTo("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");

        manager.handleDeliveryStatusUpdated(7L, 8L, "CANCELLED",
                "{\"orderId\":7,\"deliveryId\":8,\"status\":\"CANCELLED\"}");

        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.CANCELLED);
        assertThat(saga.getCompletedAt()).isNotNull();
        verify(outboxService, never()).saveCommand(eq("7"), eq(SagaManager.CMD_UPDATE_ORDER_STATUS),
                eq("7"), any());
    }

    @Test
    void deliveryCancellationFailureIsRecordedEvenAfterSagaWasCancelled() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.CANCELLED);
        saga.setDeliveryId(8L);
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        manager.handleStepFailed("DELIVERY_CANCEL", 7L, "already picked up",
                "{\"orderId\":7,\"deliveryId\":8}");

        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.FAILED);
        assertThat(saga.getSteps()).anySatisfy(step -> {
            assertThat(step.getStepName()).isEqualTo("DELIVERY_CANCEL_FAILED");
            assertThat(step.getEventType()).isEqualTo("delivery.cancel.failed");
        });
        verify(repository).save(saga);
        verifyNoInteractions(outboxService);
    }

    @Test
    void contradictoryDeliveryCreatedIdentityIsRejected() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.DELIVERY_CREATED);
        saga.setDeliveryId(8L);
        saga.addStep("DELIVERY_CREATED", "delivery.created.result",
                "{\"orderId\":7,\"deliveryId\":8}");
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        assertThrows(IllegalStateException.class,
                () -> manager.handleDeliveryCreated(
                        7L, 99L, "{\"orderId\":7,\"deliveryId\":99}"));

        verify(repository, never()).save(any());
        verifyNoInteractions(outboxService);
    }

    @Test
    void outboxPersistenceFailurePropagatesForTransactionRollbackAndRetry() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.FINDING_SHIPPER);
        saga.setDeliveryId(8L);
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));
        when(outboxService.saveCommand(eq("7"), eq(SagaManager.CMD_CACHE_SHIPPER_FOUND), eq("7"), any()))
                .thenThrow(new RuntimeException("database unavailable"));

        assertThrows(RuntimeException.class,
                () -> manager.handleShipperFound(
                        7L, 8L, "{\"orderId\":7,\"deliveryId\":8}"));
    }

    @Test
    void assignedShipperCancellationRematchesAndConvergesOrderStatus() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.SHIPPER_ASSIGNED);
        saga.setDeliveryId(8L);
        saga.setShipperId(9L);
        saga.setPayload("{\"orderId\":7,\"totalPrice\":120000,\"shippingFee\":20000,\"paymentMethod\":\"COD\"}");
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));
        String event = "{\"orderId\":7,\"deliveryId\":8,\"rejectedShipperId\":9,"
                + "\"timestamp\":1785044842775}";

        manager.handleShipperRejected(7L, 8L, 9L, event);

        ArgumentCaptor<Object> findPayload = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).saveCommand(eq("7"), eq(SagaManager.CMD_FIND_SHIPPER),
                eq("7"), findPayload.capture());
        assertThat(((JsonNode) findPayload.getValue()).has("timestamp")).isFalse();
        ArgumentCaptor<Object> statusPayload = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).saveCommand(eq("7"), eq(SagaManager.CMD_UPDATE_ORDER_STATUS),
                eq("7"), statusPayload.capture());
        assertThat(((JsonNode) statusPayload.getValue()).get("sagaStatus").asText())
                .isEqualTo("FINDING_SHIPPER");
        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.FINDING_SHIPPER);
        assertThat(saga.getShipperId()).isNull();
    }

    @Test
    void assignedShipperCancellationRejectsMismatchedIdentity() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.SHIPPER_ASSIGNED);
        saga.setDeliveryId(8L);
        saga.setShipperId(9L);
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        assertThrows(IllegalStateException.class,
                () -> manager.handleShipperRejected(7L, 8L, 10L,
                        "{\"orderId\":7,\"deliveryId\":8,\"rejectedShipperId\":10}"));

        verifyNoInteractions(outboxService);
    }

    @Test
    void delayedAcceptanceCannotResurrectARejectedShipperDuringRematch() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.FINDING_SHIPPER);
        saga.setDeliveryId(8L);
        saga.addStep("SHIPPER_ASSIGNED", "delivery.shipper-accepted",
                "{\"orderId\":7,\"deliveryId\":8,\"shipperId\":9}");
        saga.addStep("SHIPPER_REJECTED_1", "delivery.shipper-rejected",
                "{\"orderId\":7,\"deliveryId\":8,\"rejectedShipperId\":9}");
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        manager.handleShipperAccepted(7L, 8L, 9L,
                "{\"orderId\":7,\"deliveryId\":8,\"shipperId\":9}");

        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.FINDING_SHIPPER);
        assertThat(saga.getShipperId()).isNull();
        verify(repository, never()).save(any());
        verifyNoInteractions(outboxService);
    }

    @Test
    void delayedAcceptanceCannotResurrectRejectedShipperAfterNewOfferIsFound() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.SHIPPER_FOUND);
        saga.setDeliveryId(8L);
        saga.addStep("SHIPPER_REJECTED_1", "delivery.shipper-rejected",
                "{\"orderId\":7,\"deliveryId\":8,\"rejectedShipperId\":9}");
        saga.addStep("SHIPPER_FOUND", "shipper.found",
                "{\"orderId\":7,\"deliveryId\":8,\"availableShippers\":[{\"shipperId\":10}]}");
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        manager.handleShipperAccepted(7L, 8L, 9L,
                "{\"orderId\":7,\"deliveryId\":8,\"shipperId\":9}");

        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.SHIPPER_FOUND);
        assertThat(saga.getShipperId()).isNull();
        verify(repository, never()).save(any());
        verifyNoInteractions(outboxService);
    }

    @Test
    void acceptanceThatOvertakesTimeoutMayStillConvergeToAssigned() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.FINDING_SHIPPER);
        saga.setDeliveryId(8L);
        saga.addStep("SHIPPER_OFFER_TIMEOUT_1", "shipper.offer-timeout",
                "{\"orderId\":7,\"deliveryId\":8,\"rejectedShipperId\":9}");
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        manager.handleShipperAccepted(7L, 8L, 9L,
                "{\"orderId\":7,\"deliveryId\":8,\"shipperId\":9}");

        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.SHIPPER_ASSIGNED);
        assertThat(saga.getShipperId()).isEqualTo(9L);
        verify(repository).save(saga);
        verify(outboxService).saveCommand(eq("7"),
                eq(SagaManager.CMD_UPDATE_ORDER_STATUS), eq("7"), any());
    }

    @Test
    void acceptanceThatOvertakesOfferPersistedConvergesToAssigned() {
        // Delivery accepts only after committing WAIT_SHIPPER_CONFIRM, so this
        // acceptance proves the offer exists even though offer-persisted is late.
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.OFFER_PERSISTING);
        saga.setDeliveryId(8L);
        saga.addStep("SHIPPER_FOUND", "shipper.found",
                "{\"orderId\":7,\"deliveryId\":8,\"availableShippers\":[{\"shipperId\":9}]}");
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        manager.handleShipperAccepted(7L, 8L, 9L,
                "{\"orderId\":7,\"deliveryId\":8,\"shipperId\":9}");

        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.SHIPPER_ASSIGNED);
        assertThat(saga.getShipperId()).isEqualTo(9L);
        ArgumentCaptor<Object> statusPayload = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).saveCommand(eq("7"), eq(SagaManager.CMD_UPDATE_ORDER_STATUS),
                eq("7"), statusPayload.capture());
        assertThat(((JsonNode) statusPayload.getValue()).get("sagaStatus").asText())
                .isEqualTo("SHIPPER_ASSIGNED");
    }

    @Test
    void lateOfferPersistedAfterOvertakingAcceptanceIsIgnored() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.OFFER_PERSISTING);
        saga.setDeliveryId(8L);
        saga.addStep("SHIPPER_FOUND", "shipper.found",
                "{\"orderId\":7,\"deliveryId\":8,\"availableShippers\":[{\"shipperId\":9}]}");
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));
        manager.handleShipperAccepted(7L, 8L, 9L,
                "{\"orderId\":7,\"deliveryId\":8,\"shipperId\":9}");
        clearInvocations(outboxService, repository);

        manager.handleOfferPersisted(7L, 8L, "{\"orderId\":7,\"deliveryId\":8,"
                + "\"sourceCommandEventId\":\"44444444-4444-4444-4444-444444444444\","
                + "\"matchingSessionId\":\"55555555-5555-5555-5555-555555555555\","
                + "\"offeredShipperId\":9,\"offerExpiresAt\":\"2026-10-05T10:00:00\"}");

        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.SHIPPER_ASSIGNED);
        verifyNoInteractions(outboxService);
    }

    @Test
    void rejectionThatOvertakesOfferPersistedRematchesWithShipperExcluded() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.OFFER_PERSISTING);
        saga.setDeliveryId(8L);
        saga.setPayload("{\"orderId\":7,\"totalPrice\":120000,\"shippingFee\":20000,\"paymentMethod\":\"COD\"}");
        saga.addStep("SHIPPER_FOUND", "shipper.found",
                "{\"orderId\":7,\"deliveryId\":8,\"availableShippers\":[{\"shipperId\":9}]}");
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        manager.handleShipperRejected(7L, 8L, 9L,
                "{\"orderId\":7,\"deliveryId\":8,\"rejectedShipperId\":9}");

        ArgumentCaptor<Object> findPayload = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).saveCommand(eq("7"), eq(SagaManager.CMD_FIND_SHIPPER),
                eq("7"), findPayload.capture());
        assertThat(((JsonNode) findPayload.getValue()).get("excludedShipperIds").toString())
                .isEqualTo("[9]");
        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.FINDING_SHIPPER);
    }

    @Test
    void crossDeliveryShipperFoundEventIsRejected() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.FINDING_SHIPPER);
        saga.setDeliveryId(8L);
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        assertThrows(IllegalStateException.class,
                () -> manager.handleShipperFound(
                        7L, 99L, "{\"orderId\":7,\"deliveryId\":99}"));

        verify(repository, never()).save(any());
        verifyNoInteractions(outboxService);
    }

    @Test
    void differentShipperCannotOverwriteSagaAssignment() {
        SagaInstance saga = saga(7L, SagaInstance.SagaStatus.SHIPPER_ASSIGNED);
        saga.setDeliveryId(8L);
        saga.setShipperId(9L);
        saga.addStep("SHIPPER_ASSIGNED", "delivery.shipper-accepted",
                "{\"orderId\":7,\"deliveryId\":8,\"shipperId\":9}");
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        assertThrows(IllegalStateException.class,
                () -> manager.handleShipperAccepted(
                        7L, 8L, 10L,
                        "{\"orderId\":7,\"deliveryId\":8,\"shipperId\":10}"));

        assertThat(saga.getShipperId()).isEqualTo(9L);
        verify(repository, never()).save(any());
        verifyNoInteractions(outboxService);
    }

    private SagaInstance saga(Long orderId, SagaInstance.SagaStatus status) {
        SagaInstance saga = new SagaInstance();
        saga.setOrderId(orderId);
        saga.setSagaType("ORDER_CREATION");
        saga.setStatus(status);
        return saga;
    }
}
