package com.delivery.dispatch.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class DispatchPoliciesTest {

    private static final LocalDateTime NOW = LocalDateTime.of(2026, 10, 4, 12, 0);

    @Test
    void terminalStatusesAreCompletedCancelledFailed() {
        for (DispatchStatus status : DispatchStatus.values()) {
            boolean expected = status == DispatchStatus.COMPLETED || status == DispatchStatus.CANCELLED
                    || status == DispatchStatus.FAILED;
            assertEquals(expected, status.isTerminal(), status.name());
        }
    }

    @Test
    void deliveryStatusMapsToLifecycleAndRejectsUnknown() {
        assertEquals(DispatchStatus.PICKING_UP, DeliveryProgressPolicy.targetFor("PICKED_UP"));
        assertEquals(DispatchStatus.DELIVERING, DeliveryProgressPolicy.targetFor("DELIVERING"));
        assertEquals(DispatchStatus.COMPLETED, DeliveryProgressPolicy.targetFor("DELIVERED"));
        assertEquals(DispatchStatus.CANCELLED, DeliveryProgressPolicy.targetFor("CANCELLED"));
        assertEquals("Unsupported delivery status: ASSIGNED",
                assertThrows(IllegalArgumentException.class, () -> DeliveryProgressPolicy.targetFor("ASSIGNED"))
                        .getMessage());
        assertThrows(IllegalArgumentException.class, () -> DeliveryProgressPolicy.targetFor(null));
        assertTrue(DeliveryProgressPolicy.isShipperNotFoundEcho("SHIPPER_NOT_FOUND"));
        assertFalse(DeliveryProgressPolicy.isShipperNotFoundEcho("CANCELLED"));
    }

    @Test
    void deliveryTransitionsFollowStrictProgress() {
        DeliveryProgressPolicy.requireTransition(DispatchStatus.SHIPPER_ASSIGNED, DispatchStatus.PICKING_UP, 1);
        DeliveryProgressPolicy.requireTransition(DispatchStatus.PICKING_UP, DispatchStatus.DELIVERING, 1);
        DeliveryProgressPolicy.requireTransition(DispatchStatus.DELIVERING, DispatchStatus.COMPLETED, 1);
        for (DispatchStatus from : List.of(DispatchStatus.STARTED, DispatchStatus.DELIVERY_CREATED,
                DispatchStatus.FINDING_SHIPPER, DispatchStatus.SHIPPER_FOUND, DispatchStatus.SHIPPER_ASSIGNED)) {
            DeliveryProgressPolicy.requireTransition(from, DispatchStatus.CANCELLED, 1);
        }
        IllegalStateException invalid = assertThrows(IllegalStateException.class, () ->
                DeliveryProgressPolicy.requireTransition(DispatchStatus.FINDING_SHIPPER, DispatchStatus.DELIVERING, 7));
        assertEquals("Invalid saga delivery transition FINDING_SHIPPER -> DELIVERING for orderId=7",
                invalid.getMessage());
        assertThrows(IllegalStateException.class, () ->
                DeliveryProgressPolicy.requireTransition(DispatchStatus.PICKING_UP, DispatchStatus.CANCELLED, 1));
        assertThrows(IllegalStateException.class, () ->
                DeliveryProgressPolicy.requireTransition(DispatchStatus.SHIPPER_FOUND, DispatchStatus.PICKING_UP, 1));
        assertThrows(IllegalStateException.class, () ->
                DeliveryProgressPolicy.requireTransition(DispatchStatus.SHIPPER_ASSIGNED, DispatchStatus.COMPLETED, 1));
        assertThrows(IllegalStateException.class, () ->
                DeliveryProgressPolicy.requireTransition(DispatchStatus.STARTED, DispatchStatus.FAILED, 1));
    }

    @Test
    void cancellationConfirmationRequiresRecordedCancelWhileCompensating() {
        assertTrue(DeliveryProgressPolicy.isCancellationConfirmation(DispatchStatus.COMPENSATING, true));
        assertFalse(DeliveryProgressPolicy.isCancellationConfirmation(DispatchStatus.COMPENSATING, false));
        assertTrue(DeliveryProgressPolicy.isCancellationConfirmation(DispatchStatus.CANCELLED, false));
        assertTrue(DeliveryProgressPolicy.isCancellationConfirmation(DispatchStatus.FAILED, false));
        assertFalse(DeliveryProgressPolicy.isCancellationConfirmation(DispatchStatus.SHIPPER_ASSIGNED, true));
    }

    @Test
    void matchingSessionIsDeterministicPerCaseAndGeneration() {
        UUID caseId = UUID.fromString("11111111-1111-1111-1111-111111111111");
        String identity = MatchingSession.caseIdentity(caseId, 9);
        assertEquals(caseId.toString(), identity);
        assertEquals("order:9", MatchingSession.caseIdentity(null, 9));
        UUID first = MatchingSession.next(identity, 0);
        assertEquals(UUID.nameUUIDFromBytes(("saga:matching-session:" + caseId + ":1")
                .getBytes(StandardCharsets.UTF_8)), first);
        assertEquals(first, MatchingSession.next(identity, 0));
        assertNotEquals(first, MatchingSession.next(identity, 1));
        assertThrows(IllegalArgumentException.class, () -> MatchingSession.next(" ", 0));
        assertThrows(IllegalArgumentException.class, () -> MatchingSession.next(null, 0));
        assertThrows(IllegalArgumentException.class, () -> MatchingSession.next(identity, -1));
    }

    @Test
    void matchingResultFenceAcceptsLegacyCasesAndRequiresGenerationOtherwise() {
        UUID expected = UUID.randomUUID();
        assertTrue(MatchingSession.isCurrent(null, null));
        assertTrue(MatchingSession.isCurrent(expected, expected));
        assertFalse(MatchingSession.isCurrent(expected, UUID.randomUUID()));
        assertThrows(IllegalArgumentException.class, () -> MatchingSession.isCurrent(expected, null));
    }

    @Test
    void offerWindowClampsTimeoutAndTreatsUnknownStartAsDue() {
        assertEquals(180, new ShipperOffer(1, NOW, null).timeoutSeconds());
        assertEquals(1, new ShipperOffer(1, NOW, 0).timeoutSeconds());
        assertEquals(180, new ShipperOffer(1, NOW, 999).timeoutSeconds());
        assertEquals(NOW.plusSeconds(45), new ShipperOffer(1, NOW, 45).expiresAt());
        assertTrue(ShipperOffer.isDue(null, 45, NOW));
        assertFalse(ShipperOffer.isDue(NOW, 45, NOW.plusSeconds(44)));
        assertTrue(ShipperOffer.isDue(NOW, 45, NOW.plusSeconds(45)));
    }

    @Test
    void rejectionRematchExcludesCurrentThenPreviousRejectingShippers() {
        RematchPolicy.Decision decision = RematchPolicy.onRejection(30L, List.of(10L, 20L), 2);
        RematchPolicy.Rematch rematch = assertInstanceOf(RematchPolicy.Rematch.class, decision);
        assertEquals(3, rematch.attempt());
        assertEquals(List.of(30L, 10L, 20L), rematch.excludedShipperIds());
        assertThrows(UnsupportedOperationException.class, () -> rematch.excludedShipperIds().add(1L));

        assertInstanceOf(RematchPolicy.Duplicate.class, RematchPolicy.onRejection(10L, List.of(10L), 1));
        assertInstanceOf(RematchPolicy.Exhausted.class, RematchPolicy.onRejection(60L, List.of(), 5));
        RematchPolicy.Rematch legacy = assertInstanceOf(RematchPolicy.Rematch.class,
                RematchPolicy.onRejection(null, List.of(10L, 10L), 1));
        assertEquals(List.of(10L), legacy.excludedShipperIds());
    }

    @Test
    void offerTimeoutRematchExcludesAllRecordedShippersAndSharesTheLimit() {
        RematchPolicy.Rematch rematch = assertInstanceOf(RematchPolicy.Rematch.class,
                RematchPolicy.onOfferTimeout(40L, List.of(10L, 40L, 20L), 3));
        assertEquals(4, rematch.attempt());
        assertEquals(List.of(10L, 40L, 20L), rematch.excludedShipperIds());
        assertInstanceOf(RematchPolicy.Exhausted.class, RematchPolicy.onOfferTimeout(40L, List.of(), 5));
        assertEquals("Offer payload shipperId must be positive", assertThrows(IllegalStateException.class,
                () -> RematchPolicy.onOfferTimeout(0, List.of(), 0)).getMessage());
        assertEquals(new MatchingRetrySettings(5, 15, 120, 1.5), MatchingRetrySettings.REMATCH);
    }

    @Test
    void failureCompensationDependsOnStatusBeforeFailure() {
        assertEquals(new FailureCompensation(FailureCompensation.DeliveryCommand.CANCEL_DELIVERY, false, "CANCELLED"),
                FailureCompensation.forPreviousStatus(DispatchStatus.DELIVERY_CREATED));
        for (DispatchStatus matching : List.of(DispatchStatus.FINDING_SHIPPER, DispatchStatus.SHIPPER_FOUND)) {
            assertEquals(new FailureCompensation(FailureCompensation.DeliveryCommand.MARK_SHIPPER_NOT_FOUND,
                    true, "SHIPPER_NOT_FOUND"), FailureCompensation.forPreviousStatus(matching));
        }
        for (DispatchStatus other : List.of(DispatchStatus.STARTED, DispatchStatus.OFFER_PERSISTING,
                DispatchStatus.SHIPPER_ASSIGNED, DispatchStatus.COMPENSATING)) {
            assertEquals(new FailureCompensation(FailureCompensation.DeliveryCommand.NONE, false, "CANCELLED"),
                    FailureCompensation.forPreviousStatus(other));
        }
    }

    @Test
    void failureOutcomeRecordsCancelRefusalBeforeTerminalIgnore() {
        for (DispatchStatus status : List.of(DispatchStatus.COMPENSATING, DispatchStatus.CANCELLED,
                DispatchStatus.FAILED)) {
            assertEquals(FailureCompensation.Outcome.RECORD_CANCEL_REFUSAL,
                    FailureCompensation.outcome("DELIVERY_CANCEL", status));
        }
        assertEquals(FailureCompensation.Outcome.COMPENSATE,
                FailureCompensation.outcome("DELIVERY_CANCEL", DispatchStatus.FINDING_SHIPPER));
        assertEquals(FailureCompensation.Outcome.IGNORE_TERMINAL,
                FailureCompensation.outcome("DELIVERY_CANCEL", DispatchStatus.COMPLETED));
        assertEquals(FailureCompensation.Outcome.IGNORE_TERMINAL,
                FailureCompensation.outcome("TIMEOUT_STARTED", DispatchStatus.FAILED));
        assertEquals(FailureCompensation.Outcome.COMPENSATE,
                FailureCompensation.outcome("TIMEOUT_STARTED", DispatchStatus.COMPENSATING));
    }

    @Test
    void acceptanceAssignsOnlyWhileAwaitingAndFencesRejectedShippers() {
        assertEquals(AssignmentPolicy.Acceptance.ASSIGN, AssignmentPolicy.onAcceptance(
                1, DispatchStatus.SHIPPER_FOUND, 5, null, false, false));
        assertEquals(AssignmentPolicy.Acceptance.ASSIGN, AssignmentPolicy.onAcceptance(
                1, DispatchStatus.FINDING_SHIPPER, 5, null, false, false));
        assertEquals(AssignmentPolicy.Acceptance.IGNORE_REJECTED_SHIPPER, AssignmentPolicy.onAcceptance(
                1, DispatchStatus.FINDING_SHIPPER, 5, null, false, true));
        assertEquals(AssignmentPolicy.Acceptance.ASSIGN, AssignmentPolicy.onAcceptance(
                1, DispatchStatus.OFFER_PERSISTING, 5, null, false, false));
        assertEquals(AssignmentPolicy.Acceptance.IGNORE_REJECTED_SHIPPER, AssignmentPolicy.onAcceptance(
                1, DispatchStatus.OFFER_PERSISTING, 5, null, false, true));
        assertEquals(AssignmentPolicy.Acceptance.IGNORE_STATE, AssignmentPolicy.onAcceptance(
                1, DispatchStatus.DELIVERY_CREATED, 5, null, false, false));
        assertEquals(AssignmentPolicy.Acceptance.IGNORE_STATE, AssignmentPolicy.onAcceptance(
                1, DispatchStatus.CANCELLED, 5, null, false, false));
        for (DispatchStatus assigned : List.of(DispatchStatus.SHIPPER_ASSIGNED, DispatchStatus.PICKING_UP,
                DispatchStatus.DELIVERING, DispatchStatus.COMPLETED)) {
            assertEquals(AssignmentPolicy.Acceptance.REPLAY,
                    AssignmentPolicy.onAcceptance(1, assigned, 5, 5L, true, false));
            assertThrows(IllegalStateException.class,
                    () -> AssignmentPolicy.onAcceptance(1, assigned, 6, 5L, true, false));
        }
        assertThrows(IllegalStateException.class, () -> AssignmentPolicy.onAcceptance(
                1, DispatchStatus.SHIPPER_ASSIGNED, 5, 5L, false, false));
        assertThrows(IllegalStateException.class, () -> AssignmentPolicy.onAcceptance(
                1, DispatchStatus.SHIPPER_ASSIGNED, 5, null, true, false));
        assertEquals("shipperId must be positive", assertThrows(IllegalArgumentException.class,
                () -> AssignmentPolicy.onAcceptance(1, DispatchStatus.SHIPPER_FOUND, 0, null, false, false))
                .getMessage());
    }

    @Test
    void rejectionIsAcceptedWhileAwaitingOrFromTheAssignedShipper() {
        assertTrue(AssignmentPolicy.acceptsRejection(1, DispatchStatus.SHIPPER_FOUND, null, 3L));
        assertTrue(AssignmentPolicy.acceptsRejection(1, DispatchStatus.FINDING_SHIPPER, null, 3L));
        assertTrue(AssignmentPolicy.acceptsRejection(1, DispatchStatus.OFFER_PERSISTING, null, 3L));
        assertTrue(AssignmentPolicy.acceptsRejection(1, DispatchStatus.SHIPPER_ASSIGNED, 3L, 3L));
        assertFalse(AssignmentPolicy.acceptsRejection(1, DispatchStatus.PICKING_UP, 3L, 3L));
        assertThrows(IllegalStateException.class,
                () -> AssignmentPolicy.acceptsRejection(1, DispatchStatus.SHIPPER_ASSIGNED, 4L, 3L));
        assertThrows(IllegalStateException.class,
                () -> AssignmentPolicy.acceptsRejection(1, DispatchStatus.SHIPPER_ASSIGNED, null, 3L));
    }

    @Test
    void orderCancellationWaitsForKnownDeliveryAndRejectsContradictions() {
        assertEquals(AssignmentPolicy.Cancellation.CANCEL_IMMEDIATELY, AssignmentPolicy.onOrderCancelled(
                1, DispatchStatus.STARTED, false, false, false));
        assertEquals(AssignmentPolicy.Cancellation.COMPENSATE_DELIVERY, AssignmentPolicy.onOrderCancelled(
                1, DispatchStatus.SHIPPER_FOUND, false, false, true));
        assertEquals(AssignmentPolicy.Cancellation.COMPENSATE_DELIVERY, AssignmentPolicy.onOrderCancelled(
                1, DispatchStatus.COMPENSATING, false, false, true));
        assertEquals(AssignmentPolicy.Cancellation.REPLAY, AssignmentPolicy.onOrderCancelled(
                1, DispatchStatus.CANCELLED, true, true, true));
        assertEquals(AssignmentPolicy.Cancellation.REPLAY, AssignmentPolicy.onOrderCancelled(
                1, DispatchStatus.COMPENSATING, true, true, true));
        assertThrows(IllegalStateException.class, () -> AssignmentPolicy.onOrderCancelled(
                1, DispatchStatus.CANCELLED, true, false, true));
        assertEquals("Cannot cancel completed Saga for orderId=1", assertThrows(IllegalStateException.class,
                () -> AssignmentPolicy.onOrderCancelled(1, DispatchStatus.COMPLETED, false, false, true))
                .getMessage());
        assertEquals(AssignmentPolicy.Cancellation.IGNORE_FAILED, AssignmentPolicy.onOrderCancelled(
                1, DispatchStatus.FAILED, false, false, true));
    }

    @Test
    void restaurantConfirmationOpensGateOnceBeforeMatching() {
        assertTrue(AssignmentPolicy.acceptsRestaurantConfirmation(DispatchStatus.STARTED, false));
        assertTrue(AssignmentPolicy.acceptsRestaurantConfirmation(DispatchStatus.DELIVERY_CREATED, false));
        assertFalse(AssignmentPolicy.acceptsRestaurantConfirmation(DispatchStatus.DELIVERY_CREATED, true));
        assertFalse(AssignmentPolicy.acceptsRestaurantConfirmation(DispatchStatus.FINDING_SHIPPER, false));
    }

    @Test
    void offerRetirementOutcomesMapToRematchAssignOrTerminal() {
        assertEquals(OfferRetirementPolicy.Decision.REMATCH, OfferRetirementPolicy.decide("RETIRED", null));
        assertEquals(OfferRetirementPolicy.Decision.ASSIGN, OfferRetirementPolicy.decide("ASSIGNED", 9L));
        assertEquals(OfferRetirementPolicy.Decision.TERMINAL, OfferRetirementPolicy.decide("TERMINAL", null));
        assertThrows(IllegalArgumentException.class, () -> OfferRetirementPolicy.decide("ASSIGNED", null));
        assertThrows(IllegalArgumentException.class, () -> OfferRetirementPolicy.decide("ASSIGNED", 0L));
        assertThrows(IllegalArgumentException.class, () -> OfferRetirementPolicy.decide("EXPIRED", null));
        assertThrows(IllegalArgumentException.class, () -> OfferRetirementPolicy.decide(null, null));
        assertEquals(AssignmentPolicy.Acceptance.ASSIGN, AssignmentPolicy.onAcceptance(
                1, DispatchStatus.OFFER_RETIRING, 5, null, false, false));
        assertTrue(AssignmentPolicy.acceptsRejection(1, DispatchStatus.OFFER_RETIRING, null, 3L));
    }

    @Test
    void canonicalMatchingCommandRequiresIdentitiesAndPositiveCod() {
        MatchingCommandPolicy.requireCanonical(true, true, "cod", BigDecimal.ONE);
        assertEquals("Canonical matching payload is missing orderId/deliveryId",
                assertThrows(IllegalArgumentException.class, () ->
                        MatchingCommandPolicy.requireCanonical(true, false, "COD", BigDecimal.ONE)).getMessage());
        assertThrows(IllegalArgumentException.class, () ->
                MatchingCommandPolicy.requireCanonical(false, true, "COD", BigDecimal.ONE));
        assertEquals("COD is the only supported MVP matching payment method",
                assertThrows(IllegalArgumentException.class, () ->
                        MatchingCommandPolicy.requireCanonical(true, true, "ONLINE", BigDecimal.ONE)).getMessage());
        assertThrows(IllegalArgumentException.class, () ->
                MatchingCommandPolicy.requireCanonical(true, true, null, BigDecimal.ONE));
        assertEquals("Canonical COD totalPrice must be greater than zero",
                assertThrows(IllegalArgumentException.class, () ->
                        MatchingCommandPolicy.requireCanonical(true, true, "COD", BigDecimal.ZERO)).getMessage());
        assertThrows(IllegalArgumentException.class, () ->
                MatchingCommandPolicy.requireCanonical(true, true, "COD", null));
        assertEquals(NOW.plusMinutes(1), MatchingCommandPolicy.initialDeadline(NOW, 0));
        assertEquals(NOW.plusMinutes(5), MatchingCommandPolicy.initialDeadline(NOW, 5));
    }
}
