package com.delivery.dispatch.application;

import com.delivery.dispatch.application.DefaultDeliveryProgressUseCaseTest.FakeCase;
import com.delivery.dispatch.application.api.DeliveryCreationUseCase.CreatedOutcome;
import com.delivery.dispatch.application.api.DeliveryCreationUseCase.FailedOutcome;
import com.delivery.dispatch.domain.DispatchStatus;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DefaultDeliveryCreationUseCaseTest {

    private final List<String> calls = new ArrayList<>();
    private final DefaultDeliveryCreationUseCase useCase = new DefaultDeliveryCreationUseCase(
            c -> calls.add("save:" + c.status()),
            (c, event) -> calls.add("match:" + event),
            (c, cause) -> calls.add("cancelOrphan:" + cause),
            (c, status, cause) -> calls.add("order:" + status));

    @Test
    void createdDeliveryWaitsForRestaurantOrMatchesWhenAlreadyConfirmed() {
        FakeCase waiting = new FakeCase(DispatchStatus.STARTED);
        assertEquals(CreatedOutcome.AWAITING_RESTAURANT, useCase.onDeliveryCreated(waiting, 8L, "d"));
        assertEquals(DispatchStatus.DELIVERY_CREATED, waiting.status);
        assertEquals(8L, waiting.deliveryId);
        FakeCase confirmed = new FakeCase(DispatchStatus.STARTED, "RESTAURANT_CONFIRMED", "r");
        assertEquals(CreatedOutcome.MATCHING_STARTED, useCase.onDeliveryCreated(confirmed, 8L, "d"));
        assertEquals(List.of("save:DELIVERY_CREATED", "save:DELIVERY_CREATED", "match:d"), calls);
    }

    @Test
    void exactReplayIsSkippedAndSecondIdentityIsContradictory() {
        FakeCase c = new FakeCase(DispatchStatus.DELIVERY_CREATED, "DELIVERY_CREATED", "d");
        c.deliveryId = 8L;
        assertEquals(CreatedOutcome.REPLAY, useCase.onDeliveryCreated(c, 8L, "d"));
        assertThrows(IllegalStateException.class, () -> useCase.onDeliveryCreated(c, 9L, "d"));
        assertThrows(IllegalStateException.class, () -> useCase.onDeliveryCreated(c, null, "d"));
        assertTrue(calls.isEmpty());
    }

    @Test
    void lateResultAfterTerminalCaseCancelsTheOrphanDelivery() {
        for (DispatchStatus terminal : List.of(DispatchStatus.CANCELLED, DispatchStatus.FAILED)) {
            FakeCase c = new FakeCase(terminal);
            assertEquals(CreatedOutcome.ORPHAN_CANCELLED, useCase.onDeliveryCreated(c, 8L, "late"));
            assertEquals(8L, c.deliveryId);
            assertEquals(terminal, c.status);
        }
        assertEquals(List.of("save:CANCELLED", "cancelOrphan:late", "save:FAILED", "cancelOrphan:late"), calls);
        assertThrows(IllegalArgumentException.class,
                () -> useCase.onDeliveryCreated(new FakeCase(DispatchStatus.CANCELLED), 0L, "late"));
        assertThrows(IllegalArgumentException.class,
                () -> useCase.onDeliveryCreated(new FakeCase(DispatchStatus.CANCELLED), null, "late"));
        FakeCase known = new FakeCase(DispatchStatus.CANCELLED);
        known.deliveryId = 7L;
        assertThrows(IllegalStateException.class, () -> useCase.onDeliveryCreated(known, 8L, "late"));
        assertThrows(IllegalStateException.class, () -> useCase.onDeliveryCreated(
                new FakeCase(DispatchStatus.FAILED, "DELIVERY_CREATED", "d"), 8L, "late"));
    }

    @Test
    void creationFailureCompensatesOnlyWhileStarted() {
        FakeCase c = new FakeCase(DispatchStatus.STARTED);
        assertEquals(FailedOutcome.COMPENSATED, useCase.onDeliveryCreationFailed(c, "f"));
        assertEquals(DispatchStatus.FAILED, c.status);
        assertTrue(c.completed);
        assertEquals(FailedOutcome.IGNORED_STATE, useCase.onDeliveryCreationFailed(c, "f"));
        assertEquals(List.of("save:COMPENSATING", "order:CANCELLED", "save:FAILED"), calls);
    }

    @Test
    void requiresAllCollaborators() {
        assertThrows(NullPointerException.class, () -> new DefaultDeliveryCreationUseCase(
                null, (c, e) -> { }, (c, e) -> { }, (c, s, e) -> { }));
        assertThrows(NullPointerException.class, () -> new DefaultDeliveryCreationUseCase(
                c -> { }, null, (c, e) -> { }, (c, s, e) -> { }));
        assertThrows(NullPointerException.class, () -> new DefaultDeliveryCreationUseCase(
                c -> { }, (c, e) -> { }, null, (c, s, e) -> { }));
        assertThrows(NullPointerException.class, () -> new DefaultDeliveryCreationUseCase(
                c -> { }, (c, e) -> { }, (c, e) -> { }, null));
    }
}
