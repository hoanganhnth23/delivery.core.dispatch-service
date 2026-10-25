package com.delivery.dispatch.application.api;

/** Delivery's answer to create-delivery. */
public interface DeliveryCreationUseCase {

    enum CreatedOutcome { REPLAY, ORPHAN_CANCELLED, AWAITING_RESTAURANT, MATCHING_STARTED }

    enum FailedOutcome { IGNORED_STATE, COMPENSATED }

    CreatedOutcome onDeliveryCreated(DispatchCase dispatchCase, Long deliveryId, String rawEvent);

    FailedOutcome onDeliveryCreationFailed(DispatchCase dispatchCase, String rawEvent);
}
