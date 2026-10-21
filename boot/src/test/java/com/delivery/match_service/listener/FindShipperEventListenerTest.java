package com.delivery.match_service.listener;

import com.delivery.match_service.dto.event.FindShipperEvent;
import com.delivery.match_service.dto.event.MatchingDecisionTraceEvent;
import com.delivery.match_service.dto.event.ShipperFoundEvent;
import com.delivery.match_service.dto.event.ShipperNotFoundEvent;
import com.delivery.match_service.dto.request.FindNearbyShippersRequest;
import com.delivery.match_service.dto.response.NearbyShipperResponse;
import com.delivery.match_service.service.MatchService;
import com.delivery.match_service.service.MatchCancellationService;
import com.delivery.match_service.service.MatchCancellationProjectionRelay;
import com.delivery.match_service.service.MatchCommandStore;
import com.delivery.match_service.service.SettlementEligibilityClient;
import com.delivery.match_service.service.DispatchPoolService;
import com.delivery.match_service.config.MatchingBatchProperties;
import com.delivery.match_service.config.MatchingAlgorithmProperties;
import com.delivery.match_service.algorithm.BalancedEtaCanaryPolicy;
import com.delivery.match_service.metrics.BusinessMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.UUID;
import com.delivery.identity.contracts.SimulationContext;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * ✅ Test Continuous Shipper Search với Simplified Event Architecture
 * Chỉ test ShipperFoundEvent publishing, không có MatchEventService
 */
@ExtendWith(MockitoExtension.class)
class FindShipperEventListenerTest {

    @Mock
    private MatchService matchService;

    @Mock
    private MatchCommandStore matchCommandStore;

    private FindShipperEventListener listener;

    private FindShipperEvent testEvent;
    @Mock
    private MatchCancellationService matchCancellationService;
    @Mock
    private MatchCancellationProjectionRelay cancellationProjectionRelay;
    @Mock
    private SettlementEligibilityClient settlementEligibilityClient;
    private ObjectMapper objectMapper;
    @Mock
    private DispatchPoolService dispatchPoolService;

    @BeforeEach
    void setUp() {
        // ✅ Constructor Injection với simplified dependencies
        listener = new FindShipperEventListener(
                matchService, matchCommandStore, matchCancellationService, cancellationProjectionRelay,
                settlementEligibilityClient, 20);
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

        // Setup test event
        testEvent = new FindShipperEvent();
        testEvent.setEventId(UUID.fromString("11111111-1111-1111-1111-111111111111"));
        testEvent.setMatchingSessionId(UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"));
        testEvent.setDeliveryId(123L);
        testEvent.setOrderId(456L);
        testEvent.setPickupLat(10.762622);
        testEvent.setPickupLng(106.660172);
        testEvent.setDeliveryLat(10.775000);
        testEvent.setDeliveryLng(106.700000);
        testEvent.setRestaurantName("Test Restaurant");
        testEvent.setPickupAddress("123 Pickup St");
        testEvent.setDeliveryAddress("456 Delivery Ave");
        testEvent.setMaxRetryAttempts(1);
        testEvent.setInitialDelaySeconds(1);
        testEvent.setMaxDelaySeconds(1);
        testEvent.setTotalPrice(new BigDecimal("120000"));
        testEvent.setPaymentMethod("COD");
        lenient().when(matchService.tryReserveShipperOffer(
                anyLong(), anyLong(), any(UUID.class), anyInt())).thenReturn(true);
        lenient().when(settlementEligibilityClient.isCodEligible(anyLong(), any(BigDecimal.class)))
                .thenReturn(Mono.just(true));
        lenient().when(matchCommandStore.acceptFindCommand(
                anyString(), anyString(), any(FindShipperEvent.class)))
                .thenReturn(MatchCommandStore.CommandDecision.process());
        lenient().when(matchCommandStore.stageCandidate(any(), any(ShipperFoundEvent.class)))
                .thenAnswer(invocation -> invocation.getArgument(1, ShipperFoundEvent.class));
        lenient().when(matchCommandStore.stageFoundResult(any(), any(ShipperFoundEvent.class)))
                .thenReturn(true);
        lenient().when(matchCommandStore.stageNotFoundResult(
                any(), any(ShipperNotFoundEvent.class), anyBoolean()))
                .thenReturn(MatchCommandStore.TerminalResultDecision.persisted());
    }

    @Test
    void testHandleFindShipperEvent_SuccessOnFirstAttempt() {
        // Given
        NearbyShipperResponse shipper1 = createTestShipper(1L, 10.763000, 106.661000);
        NearbyShipperResponse shipper2 = createTestShipper(2L, 10.764000, 106.662000);
        List<NearbyShipperResponse> foundShippers = List.of(shipper1, shipper2);

        when(matchService.findNearbyShippers(any(FindNearbyShippersRequest.class), anyLong(), anyString()))
                .thenReturn(Mono.just(foundShippers));

        // When
        try {
            String json = objectMapper.writeValueAsString(testEvent);
            listener.handleFindShipperEvent(json, "test-topic", 0, System.currentTimeMillis()).block();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Then - Verify ShipperFoundEvent is published
        var eventCaptor = org.mockito.ArgumentCaptor.forClass(ShipperFoundEvent.class);
        verify(matchCommandStore).stageFoundResult(eq(testEvent.getEventId()), eventCaptor.capture());
        assertEquals(outcomeEventId("shipper-found"), eventCaptor.getValue().getEventId());
        assertEquals(testEvent.getMatchingSessionId().toString(), eventCaptor.getValue().getMatchingSessionId());
        assertEquals(1, eventCaptor.getValue().getAvailableShippers().size());
        assertEquals(1L, eventCaptor.getValue().getAvailableShippers().get(0).getShipperId());
    }

    @Test
    void fullBalancedEtaCanaryChangesSelectionAndExplainsTheScoreInTrace() throws Exception {
        MatchingAlgorithmProperties algorithmProperties = new MatchingAlgorithmProperties();
        algorithmProperties.setEnabled(true);
        algorithmProperties.setCanaryPercent(100);
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        FindShipperEventListener canaryListener = new FindShipperEventListener(
                matchService, matchCommandStore, matchCancellationService, cancellationProjectionRelay,
                settlementEligibilityClient, new BusinessMetrics(meterRegistry), dispatchPoolService,
                new MatchingBatchProperties(), new BalancedEtaCanaryPolicy(algorithmProperties), 20,
                Clock.systemDefaultZone());
        NearbyShipperResponse nearButBusy = createTestShipper(14L, 10.763000, 106.661000);
        nearButBusy.setDistanceKm(0.1716d);
        nearButBusy.setCompletedDeliveries(20L);
        NearbyShipperResponse fartherButFair = createTestShipper(15L, 10.764000, 106.662000);
        fartherButFair.setDistanceKm(0.3272d);
        fartherButFair.setCompletedDeliveries(0L);
        when(matchService.findNearbyShippers(any(FindNearbyShippersRequest.class), anyLong(), anyString()))
                .thenReturn(Mono.just(List.of(nearButBusy, fartherButFair)));

        canaryListener.handleFindShipperEvent(
                objectMapper.writeValueAsString(testEvent), "test-topic", 0, System.currentTimeMillis()).block();

        var foundCaptor = org.mockito.ArgumentCaptor.forClass(ShipperFoundEvent.class);
        verify(matchCommandStore).stageFoundResult(eq(testEvent.getEventId()), foundCaptor.capture());
        assertEquals(15L, foundCaptor.getValue().getAvailableShippers().get(0).getShipperId());
        var traceCaptor = org.mockito.ArgumentCaptor.forClass(MatchingDecisionTraceEvent.class);
        verify(matchCommandStore).stageDecisionTrace(eq(testEvent.getEventId()), traceCaptor.capture());
        assertEquals("balanced-eta", traceCaptor.getValue().getAlgorithmId());
        assertEquals("REAL", traceCaptor.getValue().getExecutionMode());
        assertEquals(0.6544d, traceCaptor.getValue().getCandidates().stream()
                .filter(candidate -> candidate.getShipperId().equals(15L)).findFirst().orElseThrow()
                .getCombinedScoreMinutes());
        assertEquals(1d, meterRegistry.get("delivery.matching.algorithm.decisions")
                .tag("algorithm", "balanced-eta")
                .tag("execution_mode", "REAL")
                .counter().count());
    }

    @Test
    void simulationCommandQueriesAndReservesOnlyItsScopedMatchPool() throws Exception {
        NearbyShipperResponse shipper = createTestShipper(1L, 10.763000, 106.661000);
        SimulationContext context = new SimulationContext(SimulationContext.ExecutionMode.SIMULATION,
                UUID.randomUUID(), UUID.randomUUID(), 1L);
        testEvent.setSimulationContext(context);
        when(matchService.findNearbyShippers(any(FindNearbyShippersRequest.class), anyLong(), anyString(), eq(context)))
                .thenReturn(Mono.just(List.of(shipper)));
        when(matchService.tryReserveShipperOffer(eq(1L), eq(123L), eq(testEvent.getMatchingSessionId()),
                eq(180), eq(context))).thenReturn(true);

        listener.handleFindShipperEvent(objectMapper.writeValueAsString(testEvent), "test-topic", 0,
                System.currentTimeMillis()).block();

        verify(matchService, atLeastOnce()).findNearbyShippers(any(FindNearbyShippersRequest.class), anyLong(), anyString(), eq(context));
        verify(matchService).tryReserveShipperOffer(1L, 123L, testEvent.getMatchingSessionId(), 180, context);
        verify(matchService, never()).findNearbyShippers(any(FindNearbyShippersRequest.class), anyLong(), anyString());
    }

    @Test
    void simulationCommandNeverEntersRealBatchDispatcher() throws Exception {
        MatchingBatchProperties batchProperties = new MatchingBatchProperties();
        batchProperties.setEnabled(true);
        batchProperties.setClientCapabilityRequired(true);
        FindShipperEventListener isolatedListener = new FindShipperEventListener(
                matchService, matchCommandStore, matchCancellationService, cancellationProjectionRelay,
                settlementEligibilityClient, new BusinessMetrics(new SimpleMeterRegistry()), dispatchPoolService,
                batchProperties, 20, Clock.systemDefaultZone());
        SimulationContext context = new SimulationContext(SimulationContext.ExecutionMode.SIMULATION,
                UUID.randomUUID(), UUID.randomUUID(), 1L);
        testEvent.setSimulationContext(context);
        testEvent.setBatchOfferEnabled(true);
        when(matchService.findNearbyShippers(any(FindNearbyShippersRequest.class), anyLong(), anyString(), eq(context)))
                .thenReturn(Mono.just(Collections.emptyList()));

        isolatedListener.handleFindShipperEvent(objectMapper.writeValueAsString(testEvent), "test-topic", 0,
                System.currentTimeMillis()).block();

        verify(dispatchPoolService, never()).enqueue(any(), any());
        verify(matchService, atLeastOnce()).findNearbyShippers(any(FindNearbyShippersRequest.class), anyLong(), anyString(), eq(context));
    }

    @Test
    void testHandleFindShipperEvent_SuccessAfterRetry() {
        // Given
        NearbyShipperResponse shipper1 = createTestShipper(1L, 10.763000, 106.661000);
        List<NearbyShipperResponse> foundShippers = List.of(shipper1);

        AtomicInteger subscriptions = new AtomicInteger();
        when(matchService.findNearbyShippers(any(FindNearbyShippersRequest.class), anyLong(), anyString()))
                .thenReturn(Mono.defer(() -> subscriptions.getAndIncrement() == 0
                        ? Mono.just(Collections.emptyList())
                        : Mono.just(foundShippers)));

        // When
        try {
            String json = objectMapper.writeValueAsString(testEvent);
            listener.handleFindShipperEvent(json, "test-topic", 0, System.currentTimeMillis()).block();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Then - Verify ShipperFoundEvent is published after retry
        verify(matchCommandStore).stageFoundResult(eq(testEvent.getEventId()), any(ShipperFoundEvent.class));
    }

    @Test
    void testHandleFindShipperEvent_FailAfterMaxRetries() {
        // Given - Always return empty list
        when(matchService.findNearbyShippers(any(FindNearbyShippersRequest.class), anyLong(), anyString()))
                .thenReturn(Mono.just(Collections.emptyList()));

        // When
        try {
            String json = objectMapper.writeValueAsString(testEvent);
            listener.handleFindShipperEvent(json, "test-topic", 0, System.currentTimeMillis()).block();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Then - Should publish ShipperNotFoundEvent after max retries
        var eventCaptor = org.mockito.ArgumentCaptor.forClass(ShipperNotFoundEvent.class);
        verify(matchCommandStore).stageNotFoundResult(
                eq(testEvent.getEventId()), eventCaptor.capture(), eq(false));
        assertEquals(outcomeEventId("shipper-not-found"), eventCaptor.getValue().getEventId());
        assertEquals(testEvent.getMatchingSessionId().toString(), eventCaptor.getValue().getMatchingSessionId());
        // Should not publish ShipperFoundEvent
        verify(matchCommandStore, never()).stageFoundResult(any(), any());
    }

    @Test
    void expiredSagaDeadlinePublishesTerminalNoShipperWithoutStartingRetryLoop() throws Exception {
        testEvent.setMatchingDeadlineAt(LocalDateTime.now().minusSeconds(1));

        listener.handleFindShipperEvent(
                objectMapper.writeValueAsString(testEvent), "test-topic", 0, System.currentTimeMillis()).block();

        verifyNoInteractions(matchService);
        verify(matchCommandStore).stageNotFoundResult(
                eq(testEvent.getEventId()), any(ShipperNotFoundEvent.class), eq(true));
        verify(matchCommandStore, never()).stageFoundResult(any(), any());
    }

    @Test
    void deadlineBeforeRetryPreventsASecondGeoSearch() throws Exception {
        Instant now = Instant.parse("2026-08-09T00:00:00Z");
        listener = listenerAt(Clock.fixed(now, ZoneOffset.UTC));
        testEvent.setMatchingDeadlineAt(LocalDateTime.ofInstant(now.plusMillis(500), ZoneOffset.UTC));
        when(matchService.findNearbyShippers(any(FindNearbyShippersRequest.class), anyLong(), anyString()))
                .thenReturn(Mono.just(Collections.emptyList()));

        listener.handleFindShipperEvent(
                objectMapper.writeValueAsString(testEvent), "test-topic", 0, System.currentTimeMillis()).block();

        verify(matchService, times(1)).findNearbyShippers(any(FindNearbyShippersRequest.class), anyLong(), anyString());
        verify(matchCommandStore).stageNotFoundResult(
                eq(testEvent.getEventId()), any(ShipperNotFoundEvent.class), eq(true));
        verify(matchCommandStore, never()).stageFoundResult(any(), any());
    }

    @Test
    void deadlineAfterReservationReleasesOfferBeforePublishingNoShipper() throws Exception {
        Instant beforeDeadline = Instant.parse("2026-08-09T00:00:00Z");
        listener = listenerAt(new SequenceClock(
                ZoneOffset.UTC,
                List.of(beforeDeadline, beforeDeadline, beforeDeadline, beforeDeadline,
                        beforeDeadline, beforeDeadline.plusSeconds(1))));
        testEvent.setMatchingDeadlineAt(
                LocalDateTime.ofInstant(beforeDeadline.plusMillis(500), ZoneOffset.UTC));
        NearbyShipperResponse shipper = createTestShipper(1L, 10.763000, 106.661000);
        when(matchService.findNearbyShippers(any(FindNearbyShippersRequest.class), anyLong(), anyString()))
                .thenReturn(Mono.just(List.of(shipper)));

        listener.handleFindShipperEvent(
                objectMapper.writeValueAsString(testEvent), "test-topic", 0, System.currentTimeMillis()).block();

        verify(matchService).tryReserveShipperOffer(1L, 123L, testEvent.getMatchingSessionId(), 180);
        verify(matchService).releaseShipperOffer(1L, 123L, testEvent.getMatchingSessionId());
        verify(matchCommandStore).stageNotFoundResult(
                eq(testEvent.getEventId()), any(ShipperNotFoundEvent.class), eq(true));
        var traceCaptor = org.mockito.ArgumentCaptor.forClass(MatchingDecisionTraceEvent.class);
        verify(matchCommandStore).stageDecisionTrace(eq(testEvent.getEventId()), traceCaptor.capture());
        assertEquals("RELEASED", traceCaptor.getValue().getStages().stream()
                .filter(stage -> "RESERVE".equals(stage.getName()))
                .findFirst().orElseThrow().getResult());
        verify(matchCommandStore, never()).stageFoundResult(any(), any());
    }

    @Test
    void testHandleFindShipperEvent_SystemError() {
        // Given
        when(matchService.findNearbyShippers(any(FindNearbyShippersRequest.class), anyLong(), anyString()))
                .thenReturn(Mono.error(new RuntimeException("System error")));

        String json;
        try {
            json = objectMapper.writeValueAsString(testEvent);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Infrastructure failure must not be converted to a business terminal event.
        assertThrows(RuntimeException.class,
                () -> listener.handleFindShipperEvent(
                        json, "test-topic", 0, System.currentTimeMillis()).block());
        verify(matchCommandStore, never()).stageNotFoundResult(any(), any(), anyBoolean());
        verify(matchCommandStore, never()).stageFoundResult(any(), any());
    }

    @Test
    void testHandleFindShipperEvent_InvalidEvent() {
        // Given
        FindShipperEvent invalidEvent = new FindShipperEvent();
        invalidEvent.setDeliveryId(null); // Invalid event

        String json;
        try {
            json = objectMapper.writeValueAsString(invalidEvent);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        assertThrows(IllegalStateException.class,
                () -> listener.handleFindShipperEvent(
                        json, "test-topic", 0, System.currentTimeMillis()));

        // Then
        verifyNoInteractions(matchService);
        verifyNoInteractions(matchCommandStore);
    }

    @Test
    void missingCanonicalPickupIsRejectedInsteadOfMatchingAroundDeliveryAddress() throws Exception {
        testEvent.setPickupLat(null);
        testEvent.setPickupLng(null);

        String json = objectMapper.writeValueAsString(testEvent);

        assertThrows(IllegalStateException.class,
                () -> listener.handleFindShipperEvent(
                        json, "test-topic", 0, System.currentTimeMillis()));
        verifyNoInteractions(matchService, matchCommandStore);
    }

    @Test
    void outOfCountryPickupIsRejectedBeforeQueryingGeoReplica() throws Exception {
        testEvent.setPickupLat(40.7128);
        testEvent.setPickupLng(-74.0060);

        String json = objectMapper.writeValueAsString(testEvent);

        assertThrows(IllegalStateException.class,
                () -> listener.handleFindShipperEvent(
                        json, "test-topic", 0, System.currentTimeMillis()));
        verifyNoInteractions(matchService, matchCommandStore);
    }

    @Test
    void missingCanonicalDisplayFactsIsRejectedBeforeOfferReservation() throws Exception {
        testEvent.setRestaurantName(" ");

        String json = objectMapper.writeValueAsString(testEvent);

        assertThrows(IllegalStateException.class,
                () -> listener.handleFindShipperEvent(
                        json, "test-topic", 0, System.currentTimeMillis()));
        verifyNoInteractions(matchService, matchCommandStore);
    }

    @Test
    void testCreateFindShippersRequest_WithPickupLocation() {
        // Given
        when(matchService.findNearbyShippers(any(FindNearbyShippersRequest.class), anyLong(), anyString()))
                .thenAnswer(invocation -> {
                    FindNearbyShippersRequest request = invocation.getArgument(0);
                    // Verify that pickup location is used
                    assert Double.compare(request.getLatitude(), 10.762622) == 0;
                    assert Double.compare(request.getLongitude(), 106.660172) == 0;
                    assert Double.compare(request.getRadiusKm(), 5.0) == 0;
                    assert request.getMaxShippers() == 20;

                    return Mono.just(List.of(createTestShipper(1L, 10.763000, 106.661000)));
                });

        // When
        try {
            String json = objectMapper.writeValueAsString(testEvent);
            listener.handleFindShipperEvent(json, "test-topic", 0, System.currentTimeMillis()).block();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Then
        verify(matchService).findNearbyShippers(any(FindNearbyShippersRequest.class), anyLong(),
                anyString());
        verify(matchCommandStore).stageFoundResult(eq(testEvent.getEventId()), any(ShipperFoundEvent.class));
    }

    @Test
    void skipsNearestIneligibleShipperAndOffersNextEligibleCandidate() throws Exception {
        NearbyShipperResponse first = createTestShipper(1L, 10.763000, 106.661000);
        NearbyShipperResponse second = createTestShipper(2L, 10.764000, 106.662000);
        when(matchService.findNearbyShippers(any(), anyLong(), anyString()))
                .thenReturn(Mono.just(List.of(first, second)));
        when(settlementEligibilityClient.isCodEligible(1L, testEvent.getTotalPrice()))
                .thenReturn(Mono.just(false));
        when(settlementEligibilityClient.isCodEligible(2L, testEvent.getTotalPrice()))
                .thenReturn(Mono.just(true));

        listener.handleFindShipperEvent(
                objectMapper.writeValueAsString(testEvent), "test-topic", 0,
                System.currentTimeMillis()).block();

        var eventCaptor = org.mockito.ArgumentCaptor.forClass(ShipperFoundEvent.class);
        verify(matchCommandStore, timeout(1000)).stageFoundResult(eq(testEvent.getEventId()), eventCaptor.capture());
        assertEquals(2L, eventCaptor.getValue().getAvailableShippers().get(0).getShipperId());
        assertEquals(new BigDecimal("120000"), eventCaptor.getValue().getTotalPrice());
        verify(matchService).tryReserveShipperOffer(2L, 123L, testEvent.getMatchingSessionId(), 180);
    }

    @Test
    void emitsVersionedTraceWithCandidateReasonsAndStageMeasurements() throws Exception {
        NearbyShipperResponse first = createTestShipper(1L, 10.763000, 106.661000);
        NearbyShipperResponse second = createTestShipper(2L, 10.764000, 106.662000);
        when(matchService.findNearbyShippers(any(), anyLong(), anyString()))
                .thenReturn(Mono.just(List.of(first, second)));
        when(settlementEligibilityClient.isCodEligible(1L, testEvent.getTotalPrice()))
                .thenReturn(Mono.just(false));
        when(settlementEligibilityClient.isCodEligible(2L, testEvent.getTotalPrice()))
                .thenReturn(Mono.just(true));

        listener.handleFindShipperEvent(
                objectMapper.writeValueAsString(testEvent), "test-topic", 0,
                System.currentTimeMillis()).block();

        var traceCaptor = org.mockito.ArgumentCaptor.forClass(MatchingDecisionTraceEvent.class);
        verify(matchCommandStore).stageDecisionTrace(eq(testEvent.getEventId()), traceCaptor.capture());
        MatchingDecisionTraceEvent trace = traceCaptor.getValue();
        assertEquals(MatchingDecisionTraceEvent.EVENT_VERSION, trace.getEventVersion());
        assertEquals(MatchingDecisionTraceEvent.EVENT_TYPE, trace.getEventType());
        assertEquals("nearest-cod", trace.getAlgorithmId());
        assertEquals("v1", trace.getAlgorithmVersion());
        assertEquals("SHIPPER_SELECTED", trace.getDecision());
        org.junit.jupiter.api.Assertions.assertTrue(trace.getLatencyMs() >= 0);
        assertEquals(2, trace.getCandidates().size());
        assertEquals(List.of("COD_NOT_ELIGIBLE"), trace.getCandidates().get(0).getReasons());
        assertEquals("SELECTED", trace.getCandidates().get(1).getState());
        assertEquals(2, trace.getStages().stream()
                .filter(stage -> "COD_ELIGIBILITY".equals(stage.getName()))
                .findFirst().orElseThrow().getCandidateCount());
    }

    @Test
    void emitsEmptyGeoTraceForNoShipperTerminalOutcome() throws Exception {
        when(matchService.findNearbyShippers(any(), anyLong(), anyString()))
                .thenReturn(Mono.just(Collections.emptyList()));

        listener.handleFindShipperEvent(
                objectMapper.writeValueAsString(testEvent), "test-topic", 0,
                System.currentTimeMillis()).block();

        var traceCaptor = org.mockito.ArgumentCaptor.forClass(MatchingDecisionTraceEvent.class);
        verify(matchCommandStore).stageDecisionTrace(eq(testEvent.getEventId()), traceCaptor.capture());
        MatchingDecisionTraceEvent trace = traceCaptor.getValue();
        assertEquals("SHIPPER_NOT_FOUND", trace.getDecision());
        assertEquals("EMPTY", trace.getStages().get(0).getResult());
        assertEquals("NOT_RUN", trace.getStages().get(1).getResult());
        assertEquals("NOT_RUN", trace.getStages().get(2).getResult());
    }

    @Test
    void settlementFailureIsInfrastructureFailureAndIsNotAcknowledged() throws Exception {
        testEvent.setMaxRetryAttempts(0);
        when(matchService.findNearbyShippers(any(), anyLong(), anyString()))
                .thenReturn(Mono.just(List.of(createTestShipper(1L, 10.763000, 106.661000))));
        when(settlementEligibilityClient.isCodEligible(1L, testEvent.getTotalPrice()))
                .thenReturn(Mono.error(new RuntimeException("settlement unavailable")));

        String json = objectMapper.writeValueAsString(testEvent);
        assertThrows(RuntimeException.class,
                () -> listener.handleFindShipperEvent(
                        json, "test-topic", 0, System.currentTimeMillis()).block());

        verify(matchCommandStore, never()).stageFoundResult(any(), any());
        verify(matchCommandStore, never()).stageNotFoundResult(any(), any(), anyBoolean());
    }

    @Test
    void cancelledDeliveryCannotBeResurrectedByDelayedFindCommand() throws Exception {
        when(matchCancellationService.isCancelled(123L, testEvent.getMatchingSessionId())).thenReturn(true);

        listener.handleFindShipperEvent(
                objectMapper.writeValueAsString(testEvent), "test-topic", 0,
                System.currentTimeMillis()).block();

        verifyNoInteractions(matchService);
        verify(matchCommandStore).cancelCommand(testEvent.getEventId());
    }

    @Test
    void cancellationAfterReservationReleasesOfferBeforeSkippingPublish() throws Exception {
        NearbyShipperResponse shipper = createTestShipper(1L, 10.763000, 106.661000);
        when(matchService.findNearbyShippers(any(), anyLong(), anyString()))
                .thenReturn(Mono.just(List.of(shipper)));
        when(matchCancellationService.isCancelled(123L, testEvent.getMatchingSessionId()))
                .thenReturn(false, false, false, true);

        listener.handleFindShipperEvent(
                objectMapper.writeValueAsString(testEvent), "test-topic", 0,
                System.currentTimeMillis()).block();

        verify(matchService).tryReserveShipperOffer(1L, 123L, testEvent.getMatchingSessionId(), 180);
        verify(matchService).releaseShipperOffer(1L, 123L, testEvent.getMatchingSessionId());
        verify(matchCommandStore, never()).stageFoundResult(any(), any());
        verify(matchCommandStore, never()).stageNotFoundResult(any(), any(), anyBoolean());
    }

    @Test
    void cancellationStoreFailurePropagatesToKafkaRetry() throws Exception {
        when(matchCancellationService.isCancelled(123L, testEvent.getMatchingSessionId()))
                .thenThrow(new IllegalStateException("redis unavailable"));

        String json = objectMapper.writeValueAsString(testEvent);
        assertThrows(IllegalStateException.class,
                () -> listener.handleFindShipperEvent(
                        json, "test-topic", 0, System.currentTimeMillis()));

        verifyNoInteractions(matchService);
        verify(matchCommandStore, never()).stageFoundResult(any(), any());
        verify(matchCommandStore, never()).stageNotFoundResult(any(), any(), anyBoolean());
    }

    @Test
    void doesNotInventShipperProfileFactsForGeoCandidate() throws Exception {
        NearbyShipperResponse shipper = createTestShipper(1L, 10.763000, 106.661000);
        shipper.setShipperName(null);
        shipper.setShipperPhone(null);
        when(matchService.findNearbyShippers(any(), anyLong(), anyString()))
                .thenReturn(Mono.just(List.of(shipper)));

        listener.handleFindShipperEvent(
                objectMapper.writeValueAsString(testEvent), "test-topic", 0,
                System.currentTimeMillis()).block();

        var eventCaptor = org.mockito.ArgumentCaptor.forClass(ShipperFoundEvent.class);
        verify(matchCommandStore).stageFoundResult(eq(testEvent.getEventId()), eventCaptor.capture());
        var selected = eventCaptor.getValue().getAvailableShippers().get(0);
        assertNull(selected.getShipperName());
        assertNull(selected.getShipperPhone());
        assertNull(selected.getRating());
    }

    @Test
    void stopMatchingAcknowledgesAfterDurableFenceWhenRedisProjectionIsPending() {
        org.springframework.kafka.support.Acknowledgment acknowledgment =
                mock(org.springframework.kafka.support.Acknowledgment.class);
        UUID matchingSessionId = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        when(cancellationProjectionRelay.projectNow(123L, matchingSessionId)).thenReturn(false);

        listener.handleStopMatchingCommand(
                "{\"eventId\":\"22222222-2222-2222-2222-222222222222\","
                        + "\"orderId\":456,\"deliveryId\":123,"
                        + "\"matchingSessionId\":\"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\"}",
                acknowledgment);

        verify(matchCommandStore).recordStopMatching(
                UUID.fromString("22222222-2222-2222-2222-222222222222"),
                456L, 123L, matchingSessionId,
                "{\"eventId\":\"22222222-2222-2222-2222-222222222222\","
                        + "\"orderId\":456,\"deliveryId\":123,"
                        + "\"matchingSessionId\":\"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\"}");
        verify(cancellationProjectionRelay).projectNow(123L, matchingSessionId);
        verify(acknowledgment).acknowledge();
    }

    private NearbyShipperResponse createTestShipper(Long shipperId, Double lat, Double lng) {
        NearbyShipperResponse shipper = new NearbyShipperResponse();
        shipper.setShipperId(shipperId);
        shipper.setShipperName("Shipper " + shipperId);
        shipper.setShipperPhone("090123456" + shipperId);
        shipper.setLatitude(lat);
        shipper.setLongitude(lng);
        shipper.setDistanceKm(1.2);
        shipper.setOnline(true);
        return shipper;
    }

    private String outcomeEventId(String outcome) {
        return UUID.nameUUIDFromBytes(("match:" + outcome + ":" + testEvent.getEventId())
                .getBytes(StandardCharsets.UTF_8)).toString();
    }

    private FindShipperEventListener listenerAt(Clock clock) {
        return new FindShipperEventListener(
                matchService,
                matchCommandStore,
                matchCancellationService,
                cancellationProjectionRelay,
                settlementEligibilityClient,
                new BusinessMetrics(new SimpleMeterRegistry()),
                20,
                clock);
    }

    private static final class SequenceClock extends Clock {
        private final ZoneId zone;
        private final List<Instant> instants;
        private final AtomicInteger reads = new AtomicInteger();

        private SequenceClock(ZoneId zone, List<Instant> instants) {
            this.zone = zone;
            this.instants = instants;
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId requestedZone) {
            return new SequenceClock(requestedZone, instants);
        }

        @Override
        public Instant instant() {
            return instants.get(Math.min(reads.getAndIncrement(), instants.size() - 1));
        }
    }
}
