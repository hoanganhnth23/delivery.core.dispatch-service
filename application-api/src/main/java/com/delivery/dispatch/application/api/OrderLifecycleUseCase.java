package com.delivery.dispatch.application.api;

/** Order- and Restaurant-owned facts applied to a locked case. */
public interface OrderLifecycleUseCase {

    enum RestaurantOutcome { ALREADY_CONFIRMED, IGNORED_STATE, AWAITING_DELIVERY, MATCHING_STARTED }

    enum CancellationOutcome { REPLAY, IGNORED_FAILED, CANCELLED, COMPENSATING }

    RestaurantOutcome confirmRestaurant(DispatchCase dispatchCase, String rawEvent);

    CancellationOutcome cancelOrder(DispatchCase dispatchCase, String rawEvent);
}
