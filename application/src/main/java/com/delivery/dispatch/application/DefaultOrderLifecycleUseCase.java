package com.delivery.dispatch.application;

import com.delivery.dispatch.application.api.CompensationCommands;
import com.delivery.dispatch.application.api.DispatchCase;
import com.delivery.dispatch.application.api.DispatchCaseStore;
import com.delivery.dispatch.application.api.EventEquality;
import com.delivery.dispatch.application.api.MatchingCommands;
import com.delivery.dispatch.application.api.OrderLifecycleUseCase;
import com.delivery.dispatch.domain.AssignmentPolicy;
import com.delivery.dispatch.domain.DispatchStatus;

import java.util.Objects;

/**
 * Restaurant confirmation opens the matching gate (immediately when Delivery
 * already exists); order cancellation cancels at once when no Delivery is
 * known, otherwise compensates and waits for Delivery's durable CANCELLED.
 */
public final class DefaultOrderLifecycleUseCase implements OrderLifecycleUseCase {

    static final String RESTAURANT_CONFIRMED = "RESTAURANT_CONFIRMED";
    static final String ORDER_CANCELLED = "ORDER_CANCELLED";

    private final DispatchCaseStore store;
    private final MatchingCommands matching;
    private final CompensationCommands compensation;
    private final EventEquality equality;

    public DefaultOrderLifecycleUseCase(DispatchCaseStore store, MatchingCommands matching,
                                        CompensationCommands compensation, EventEquality equality) {
        this.store = Objects.requireNonNull(store, "store");
        this.matching = Objects.requireNonNull(matching, "matching");
        this.compensation = Objects.requireNonNull(compensation, "compensation");
        this.equality = Objects.requireNonNull(equality, "equality");
    }

    @Override
    public RestaurantOutcome confirmRestaurant(DispatchCase dispatchCase, String rawEvent) {
        boolean alreadyConfirmed = dispatchCase.history().has(RESTAURANT_CONFIRMED);
        if (!AssignmentPolicy.acceptsRestaurantConfirmation(dispatchCase.status(), alreadyConfirmed)) {
            return AssignmentPolicy.acceptsRestaurantConfirmation(dispatchCase.status(), false)
                    ? RestaurantOutcome.ALREADY_CONFIRMED
                    : RestaurantOutcome.IGNORED_STATE;
        }
        dispatchCase.record(RESTAURANT_CONFIRMED, "restaurant.order-confirmed", rawEvent);
        store.save(dispatchCase);
        if (dispatchCase.status() != DispatchStatus.DELIVERY_CREATED || dispatchCase.deliveryId() == null) {
            // Delivery is not ready; delivery-created will open the gate when it arrives.
            return RestaurantOutcome.AWAITING_DELIVERY;
        }
        String deliveryEvent = dispatchCase.history().latest("DELIVERY_CREATED");
        if (deliveryEvent == null) {
            throw new IllegalStateException(
                    "Saga DELIVERY_CREATED is missing its canonical delivery.created.result payload");
        }
        matching.startMatching(dispatchCase, deliveryEvent);
        return RestaurantOutcome.MATCHING_STARTED;
    }

    @Override
    public CancellationOutcome cancelOrder(DispatchCase dispatchCase, String rawEvent) {
        AssignmentPolicy.Cancellation cancellation = AssignmentPolicy.onOrderCancelled(
                dispatchCase.orderId(), dispatchCase.status(),
                dispatchCase.history().has(ORDER_CANCELLED),
                equality.same(dispatchCase.history().latest(ORDER_CANCELLED), rawEvent),
                dispatchCase.deliveryId() != null);
        CancellationOutcome outcome;
        switch (cancellation) {
            case REPLAY -> {
                return CancellationOutcome.REPLAY;
            }
            case IGNORE_FAILED -> {
                // A failure compensation commands Order to CANCELLED; that echo is not a new request.
                return CancellationOutcome.IGNORED_FAILED;
            }
            case CANCEL_IMMEDIATELY -> {
                dispatchCase.transitionTo(DispatchStatus.CANCELLED);
                dispatchCase.markCompleted();
                outcome = CancellationOutcome.CANCELLED;
            }
            default -> {
                dispatchCase.transitionTo(DispatchStatus.COMPENSATING);
                dispatchCase.clearCompletion();
                outcome = CancellationOutcome.COMPENSATING;
            }
        }
        dispatchCase.record(ORDER_CANCELLED, "order.cancelled", rawEvent);
        store.save(dispatchCase);
        compensation.cancelDeliveryAndStopMatching(dispatchCase, rawEvent);
        return outcome;
    }
}
