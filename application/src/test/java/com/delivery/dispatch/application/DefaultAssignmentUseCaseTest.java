package com.delivery.dispatch.application;

import com.delivery.dispatch.application.DefaultDeliveryProgressUseCaseTest.FakeCase;
import com.delivery.dispatch.application.api.AssignmentUseCase.AcceptanceOutcome;
import com.delivery.dispatch.application.api.AssignmentUseCase.RejectionOutcome;
import com.delivery.dispatch.application.api.DispatchCase;
import com.delivery.dispatch.application.api.OfferCommands;
import com.delivery.dispatch.domain.DispatchStatus;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DefaultAssignmentUseCaseTest {

    private final List<String> calls = new ArrayList<>();
    private final OfferCommands offers = new OfferCommands() {
        @Override public void requestOfferPersistence(DispatchCase c, String found) { calls.add("persist"); }
        @Override public void markShipperNotFound(DispatchCase c, String cause) { calls.add("notFound:" + cause); }
        @Override public String startPreparedRematch(DispatchCase c, String prepared) { return "find"; }
    };
    private final DefaultAssignmentUseCase useCase = new DefaultAssignmentUseCase(
            c -> calls.add("save:" + c.status()),
            (c, cause, excluded) -> calls.add("rematch:" + excluded),
            offers,
            (c, status, cause) -> calls.add("order:" + status));

    @Test
    void acceptanceAssignsWhileAwaitingAndForwardsToOrder() {
        FakeCase c = new FakeCase(DispatchStatus.SHIPPER_FOUND);
        assertEquals(AcceptanceOutcome.ASSIGNED, useCase.onAccepted(c, 9L, "a"));
        assertEquals(DispatchStatus.SHIPPER_ASSIGNED, c.status);
        assertEquals(9L, c.shipperId);
        assertEquals(AcceptanceOutcome.REPLAY, useCase.onAccepted(c, 9L, "a"));
        assertThrows(IllegalStateException.class, () -> useCase.onAccepted(c, 10L, "b"));
        assertEquals(List.of("save:SHIPPER_ASSIGNED", "order:SHIPPER_ASSIGNED"), calls);
    }

    @Test
    void acceptanceFromRejectingShipperOrWrongStateIsIgnored() {
        assertEquals(AcceptanceOutcome.IGNORED_REJECTED_SHIPPER, useCase.onAccepted(
                new FakeCase(DispatchStatus.FINDING_SHIPPER, "SHIPPER_REJECTED_1", "9"), 9L, "a"));
        assertEquals(AcceptanceOutcome.IGNORED_STATE,
                useCase.onAccepted(new FakeCase(DispatchStatus.CANCELLED), 9L, "a"));
        assertEquals("shipperId must be positive", assertThrows(IllegalArgumentException.class,
                () -> useCase.onAccepted(new FakeCase(DispatchStatus.SHIPPER_FOUND), 0L, "a")).getMessage());
        assertThrows(IllegalArgumentException.class,
                () -> useCase.onAccepted(new FakeCase(DispatchStatus.SHIPPER_FOUND), null, "a"));
        assertTrue(calls.isEmpty());
    }

    @Test
    void rejectionRematchesWithExclusionsAndClearsTheAssignment() {
        FakeCase c = new FakeCase(DispatchStatus.SHIPPER_ASSIGNED, "SHIPPER_REJECTED_1", "5");
        c.shipperId = 9L;
        assertEquals(RejectionOutcome.REMATCHED, useCase.onRejected(c, 9L, "r"));
        assertEquals(DispatchStatus.FINDING_SHIPPER, c.status);
        assertNull(c.shipperId);
        assertTrue(c.history().has("SHIPPER_REJECTED_2"));
        assertEquals(List.of("save:FINDING_SHIPPER", "rematch:[9, 5]", "save:FINDING_SHIPPER",
                "order:FINDING_SHIPPER"), calls);
    }

    @Test
    void duplicateIgnoredAndLimitFailsAsShipperNotFound() {
        assertEquals(RejectionOutcome.DUPLICATE, useCase.onRejected(
                new FakeCase(DispatchStatus.FINDING_SHIPPER, "SHIPPER_REJECTED_1", "9"), 9L, "r"));
        FakeCase exhausted = new FakeCase(DispatchStatus.SHIPPER_FOUND,
                "SHIPPER_REJECTED_1", "1", "SHIPPER_REJECTED_2", "2", "SHIPPER_REJECTED_3", "3",
                "SHIPPER_REJECTED_4", "4", "SHIPPER_REJECTED_5", "5");
        assertEquals(RejectionOutcome.EXHAUSTED, useCase.onRejected(exhausted, 6L, "r"));
        assertEquals(DispatchStatus.FAILED, exhausted.status);
        assertTrue(exhausted.completed);
        assertEquals(List.of("save:FAILED", "notFound:r", "order:SHIPPER_NOT_FOUND"), calls);
    }

    @Test
    void rejectionOutsideAwaitingStatesIsIgnoredAndMismatchFails() {
        assertEquals(RejectionOutcome.IGNORED_STATE,
                useCase.onRejected(new FakeCase(DispatchStatus.PICKING_UP), 9L, "r"));
        FakeCase assigned = new FakeCase(DispatchStatus.SHIPPER_ASSIGNED);
        assigned.shipperId = 9L;
        assertThrows(IllegalStateException.class, () -> useCase.onRejected(assigned, 10L, "r"));
        assertTrue(calls.isEmpty());
    }

    @Test
    void requiresAllCollaborators() {
        assertThrows(NullPointerException.class,
                () -> new DefaultAssignmentUseCase(null, (c, e, x) -> { }, offers, (c, s, e) -> { }));
        assertThrows(NullPointerException.class,
                () -> new DefaultAssignmentUseCase(c -> { }, null, offers, (c, s, e) -> { }));
        assertThrows(NullPointerException.class,
                () -> new DefaultAssignmentUseCase(c -> { }, (c, e, x) -> { }, null, (c, s, e) -> { }));
        assertThrows(NullPointerException.class,
                () -> new DefaultAssignmentUseCase(c -> { }, (c, e, x) -> { }, offers, null));
    }
}
