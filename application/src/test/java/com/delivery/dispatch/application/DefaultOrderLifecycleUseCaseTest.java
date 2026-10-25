package com.delivery.dispatch.application;

import com.delivery.dispatch.application.DefaultDeliveryProgressUseCaseTest.FakeCase;
import com.delivery.dispatch.application.api.OrderLifecycleUseCase.CancellationOutcome;
import com.delivery.dispatch.application.api.OrderLifecycleUseCase.RestaurantOutcome;
import com.delivery.dispatch.domain.DispatchStatus;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

class DefaultOrderLifecycleUseCaseTest {

    private final List<String> calls = new ArrayList<>();
    private final DefaultOrderLifecycleUseCase useCase = new DefaultOrderLifecycleUseCase(
            c -> calls.add("save:" + c.status()),
            (c, deliveryEvent) -> calls.add("match:" + deliveryEvent),
            (c, cause) -> calls.add("compensate:" + cause),
            Objects::equals);

    private static FakeCase withDelivery(FakeCase c) {
        c.deliveryId = 8L;
        return c;
    }

    @Test
    void confirmationWithReadyDeliveryStartsMatchingFromItsCanonicalResult() {
        FakeCase c = withDelivery(new FakeCase(DispatchStatus.DELIVERY_CREATED, "DELIVERY_CREATED", "created"));
        assertEquals(RestaurantOutcome.MATCHING_STARTED, useCase.confirmRestaurant(c, "confirm"));
        assertTrue(c.history().has("RESTAURANT_CONFIRMED"));
        assertEquals(List.of("save:DELIVERY_CREATED", "match:created"), calls);
    }

    @Test
    void confirmationBeforeDeliveryIsOnlyRecorded() {
        assertEquals(RestaurantOutcome.AWAITING_DELIVERY,
                useCase.confirmRestaurant(new FakeCase(DispatchStatus.STARTED), "confirm"));
        assertEquals(RestaurantOutcome.AWAITING_DELIVERY,
                useCase.confirmRestaurant(new FakeCase(DispatchStatus.DELIVERY_CREATED), "confirm"));
        assertEquals(List.of("save:STARTED", "save:DELIVERY_CREATED"), calls);
    }

    @Test
    void confirmationIsIdempotentAndIgnoredAfterMatchingStarted() {
        assertEquals(RestaurantOutcome.ALREADY_CONFIRMED, useCase.confirmRestaurant(
                new FakeCase(DispatchStatus.STARTED, "RESTAURANT_CONFIRMED", "c"), "c"));
        assertEquals(RestaurantOutcome.IGNORED_STATE,
                useCase.confirmRestaurant(new FakeCase(DispatchStatus.FINDING_SHIPPER), "c"));
        assertTrue(calls.isEmpty());
    }

    @Test
    void readyDeliveryWithoutCanonicalResultFailsClosed() {
        FakeCase c = withDelivery(new FakeCase(DispatchStatus.DELIVERY_CREATED));
        assertThrows(IllegalStateException.class, () -> useCase.confirmRestaurant(c, "confirm"));
    }

    @Test
    void cancellationWithoutDeliveryCompletesImmediately() {
        FakeCase c = new FakeCase(DispatchStatus.STARTED);
        assertEquals(CancellationOutcome.CANCELLED, useCase.cancelOrder(c, "cancel"));
        assertEquals(DispatchStatus.CANCELLED, c.status);
        assertTrue(c.completed);
        assertEquals(List.of("save:CANCELLED", "compensate:cancel"), calls);
    }

    @Test
    void cancellationWithKnownDeliveryCompensatesAndAwaitsDelivery() {
        FakeCase c = withDelivery(new FakeCase(DispatchStatus.SHIPPER_FOUND));
        c.completed = true;
        assertEquals(CancellationOutcome.COMPENSATING, useCase.cancelOrder(c, "cancel"));
        assertEquals(DispatchStatus.COMPENSATING, c.status);
        assertTrue(c.completionCleared);
        assertEquals(List.of("save:COMPENSATING", "compensate:cancel"), calls);
        assertEquals(CancellationOutcome.REPLAY, useCase.cancelOrder(c, "cancel"));
        assertThrows(IllegalStateException.class, () -> useCase.cancelOrder(c, "other"));
    }

    @Test
    void failedCaseIgnoresItsOwnCancellationConsequenceAndCompletedRejects() {
        assertEquals(CancellationOutcome.IGNORED_FAILED,
                useCase.cancelOrder(new FakeCase(DispatchStatus.FAILED), "cancel"));
        assertThrows(IllegalStateException.class,
                () -> useCase.cancelOrder(new FakeCase(DispatchStatus.COMPLETED), "cancel"));
        assertTrue(calls.isEmpty());
    }

    @Test
    void requiresAllCollaborators() {
        assertThrows(NullPointerException.class, () -> new DefaultOrderLifecycleUseCase(
                null, (c, e) -> { }, (c, e) -> { }, Objects::equals));
        assertThrows(NullPointerException.class, () -> new DefaultOrderLifecycleUseCase(
                c -> { }, null, (c, e) -> { }, Objects::equals));
        assertThrows(NullPointerException.class, () -> new DefaultOrderLifecycleUseCase(
                c -> { }, (c, e) -> { }, null, Objects::equals));
        assertThrows(NullPointerException.class, () -> new DefaultOrderLifecycleUseCase(
                c -> { }, (c, e) -> { }, (c, e) -> { }, null));
    }
}
