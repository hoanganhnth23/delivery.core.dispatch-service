package com.delivery.saga_orchestrator_service.service;

import com.delivery.saga_orchestrator_service.entity.SagaInstance;
import com.delivery.saga_orchestrator_service.entity.SagaStep;
import com.delivery.saga_orchestrator_service.repository.SagaInstanceRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class SagaManagerMatchingGenerationTest {

    private static final UUID FIRST_SESSION =
            UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static final UUID SECOND_SESSION =
            UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

    private final ObjectMapper objectMapper = new ObjectMapper();

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
    void initialFindCarriesAndPersistsASagaOwnedMatchingSession() throws Exception {
        SagaInstance saga = matchingSaga(SagaInstance.SagaStatus.DELIVERY_CREATED);
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        manager.handleRestaurantConfirmed(7L,
                "{\"eventId\":\"11111111-1111-1111-1111-111111111111\",\"orderId\":7}");

        ArgumentCaptor<Object> findPayload = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).saveCommand(eq("7"), eq(SagaManager.CMD_FIND_SHIPPER),
                eq("7"), findPayload.capture());
        JsonNode command = (JsonNode) findPayload.getValue();
        UUID matchingSessionId = UUID.fromString(command.path("matchingSessionId").asText());

        List<SagaStep> starts = matchingStarts(saga);
        assertThat(starts).hasSize(1);
        assertThat(objectMapper.readTree(starts.get(0).getEventData())
                .path("matchingSessionId").asText()).isEqualTo(matchingSessionId.toString());
        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.FINDING_SHIPPER);
    }

    @Test
    void rejectionRematchCreatesANewMatchingSessionWithoutChangingTheDeadline() throws Exception {
        SagaInstance saga = matchingSaga(SagaInstance.SagaStatus.SHIPPER_FOUND);
        saga.addStep("MATCHING_STARTED", SagaManager.CMD_FIND_SHIPPER,
                matchingStart(FIRST_SESSION, "2026-08-09T12:00:00"));
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        manager.handleShipperRejected(7L, 8L, 9L,
                "{\"eventId\":\"22222222-2222-2222-2222-222222222222\","
                        + "\"orderId\":7,\"deliveryId\":8,\"rejectedShipperId\":9}");

        ArgumentCaptor<Object> findPayload = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).saveCommand(eq("7"), eq(SagaManager.CMD_FIND_SHIPPER),
                eq("7"), findPayload.capture());
        JsonNode rematch = (JsonNode) findPayload.getValue();
        UUID rematchSession = UUID.fromString(rematch.path("matchingSessionId").asText());

        assertThat(rematchSession).isNotEqualTo(FIRST_SESSION);
        assertThat(rematch.path("matchingDeadlineAt").asText()).isEqualTo("2026-08-09T12:00:00");
        List<SagaStep> starts = matchingStarts(saga);
        assertThat(starts).hasSize(2);
        assertThat(objectMapper.readTree(starts.get(1).getEventData())
                .path("matchingSessionId").asText()).isEqualTo(rematchSession.toString());
    }

    @Test
    void cancellationTargetsTheCurrentRematchGeneration() {
        SagaInstance saga = matchingSaga(SagaInstance.SagaStatus.FINDING_SHIPPER);
        saga.addStep("MATCHING_STARTED", SagaManager.CMD_FIND_SHIPPER,
                matchingStart(FIRST_SESSION, "2026-08-09T12:00:00"));
        saga.addStep("MATCHING_STARTED", SagaManager.CMD_FIND_SHIPPER,
                matchingStart(SECOND_SESSION, "2026-08-09T12:00:00"));
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        manager.handleOrderCancelled(7L,
                "{\"eventId\":\"33333333-3333-3333-3333-333333333333\",\"orderId\":7}");

        ArgumentCaptor<Object> stopPayload = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).saveCommand(eq("7"), eq(SagaManager.CMD_STOP_MATCHING),
                eq("7"), stopPayload.capture());
        JsonNode stop = (JsonNode) stopPayload.getValue();
        assertThat(stop.path("deliveryId").asLong()).isEqualTo(8L);
        assertThat(stop.path("matchingSessionId").asText()).isEqualTo(SECOND_SESSION.toString());
        assertThat(stop.path("causeEventId").asText())
                .isEqualTo("33333333-3333-3333-3333-333333333333");
    }

    @Test
    void staleFoundAndNotFoundGenerationsDoNotAdvanceTheCurrentRematch() {
        SagaInstance saga = matchingSaga(SagaInstance.SagaStatus.FINDING_SHIPPER);
        saga.addStep("MATCHING_STARTED", SagaManager.CMD_FIND_SHIPPER,
                matchingStart(FIRST_SESSION, "2026-08-09T12:00:00"));
        saga.addStep("MATCHING_STARTED", SagaManager.CMD_FIND_SHIPPER,
                matchingStart(SECOND_SESSION, "2026-08-09T12:00:00"));
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        manager.handleShipperFound(7L, 8L,
                matchingResult("44444444-4444-4444-4444-444444444444", FIRST_SESSION));
        manager.handleShipperNotFound(7L, 8L,
                matchingResult("55555555-5555-5555-5555-555555555555", FIRST_SESSION));

        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.FINDING_SHIPPER);
        assertThat(saga.getSteps()).noneMatch(step -> "SHIPPER_FOUND".equals(step.getStepName())
                || "SHIPPER_NOT_FOUND".equals(step.getStepName()));
        verify(repository, never()).save(any());
        verifyNoInteractions(outboxService);
    }

    @Test
    void orderWaitsForDeliveryOfferPersistenceConfirmation() {
        SagaInstance saga = matchingSaga(SagaInstance.SagaStatus.FINDING_SHIPPER);
        saga.addStep("MATCHING_STARTED", SagaManager.CMD_FIND_SHIPPER,
                matchingStart(FIRST_SESSION, "2026-08-09T12:00:00"));
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));
        UUID cacheCommandId = UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc");
        when(outboxService.saveCommand(eq("7"), eq(SagaManager.CMD_CACHE_SHIPPER_FOUND), eq("7"), any()))
                .thenReturn(cacheCommandId);

        manager.handleShipperFound(7L, 8L, matchingFound(cacheCommandId, FIRST_SESSION));

        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.OFFER_PERSISTING);
        verify(outboxService, never()).saveCommand(eq("7"), eq(SagaManager.CMD_UPDATE_ORDER_STATUS), eq("7"), any());

        manager.handleOfferPersisted(7L, 8L, "{" +
                "\"eventId\":\"dddddddd-dddd-dddd-dddd-dddddddddddd\"," +
                "\"sourceCommandEventId\":\"" + cacheCommandId + "\"," +
                "\"orderId\":7,\"deliveryId\":8," +
                "\"matchingSessionId\":\"" + FIRST_SESSION + "\"," +
                "\"offeredShipperId\":9,\"offerExpiresAt\":\"2026-08-09T12:03:00\"}");

        assertThat(saga.getStatus()).isEqualTo(SagaInstance.SagaStatus.SHIPPER_FOUND);
        ArgumentCaptor<Object> statusCommand = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).saveCommand(eq("7"), eq(SagaManager.CMD_UPDATE_ORDER_STATUS),
                eq("7"), statusCommand.capture());
        assertThat(((JsonNode) statusCommand.getValue()).path("sagaStatus").asText())
                .isEqualTo("WAIT_SHIPPER_CONFIRM");
    }

    @Test
    void legacyMatchingStepNeverEmitsABroadStopCommandDuringRollout() {
        SagaInstance saga = matchingSaga(SagaInstance.SagaStatus.FINDING_SHIPPER);
        saga.addStep("MATCHING_STARTED", SagaManager.CMD_FIND_SHIPPER,
                "{\"orderId\":7,\"deliveryId\":8}");
        when(repository.findByOrderIdForUpdate(7L)).thenReturn(Optional.of(saga));

        manager.handleOrderCancelled(7L,
                "{\"eventId\":\"66666666-6666-6666-6666-666666666666\",\"orderId\":7}");

        verify(outboxService).saveCommand(eq("7"), eq(SagaManager.CMD_CANCEL_DELIVERY), eq("7"), any());
        verify(outboxService, never()).saveCommand(eq("7"), eq(SagaManager.CMD_STOP_MATCHING),
                eq("7"), any());
    }

    private SagaInstance matchingSaga(SagaInstance.SagaStatus status) {
        SagaInstance saga = new SagaInstance();
        saga.setOrderId(7L);
        saga.setDeliveryId(8L);
        saga.setSagaType("ORDER_CREATION");
        saga.setStatus(status);
        saga.setPayload("{\"orderId\":7,\"totalPrice\":120000,\"shippingFee\":20000,"
                + "\"paymentMethod\":\"COD\",\"restaurantId\":3,\"restaurantName\":\"Test\"}");
        saga.addStep("DELIVERY_CREATED", "delivery.created.result",
                "{\"orderId\":7,\"deliveryId\":8,\"pickupAddress\":\"Pickup\","
                        + "\"pickupLat\":10.75,\"pickupLng\":106.67,"
                        + "\"deliveryAddress\":\"Dropoff\",\"deliveryLat\":10.76,"
                        + "\"deliveryLng\":106.68}");
        return saga;
    }

    private List<SagaStep> matchingStarts(SagaInstance saga) {
        return saga.getSteps().stream()
                .filter(step -> "MATCHING_STARTED".equals(step.getStepName()))
                .toList();
    }

    private String matchingStart(UUID matchingSessionId, String deadline) {
        return "{\"orderId\":7,\"deliveryId\":8,\"matchingSessionId\":\""
                + matchingSessionId + "\",\"matchingDeadlineAt\":\"" + deadline + "\"}";
    }

    private String matchingResult(String eventId, UUID matchingSessionId) {
        return "{\"eventId\":\"" + eventId + "\",\"orderId\":7,\"deliveryId\":8,"
                + "\"matchingSessionId\":\"" + matchingSessionId + "\"}";
    }

    private String matchingFound(UUID eventId, UUID matchingSessionId) {
        return "{\"eventId\":\"" + eventId + "\",\"orderId\":7,\"deliveryId\":8,"
                + "\"matchingSessionId\":\"" + matchingSessionId + "\","
                + "\"availableShippers\":[{\"shipperId\":9}]}";
    }
}
