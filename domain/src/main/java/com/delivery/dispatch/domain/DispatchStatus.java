package com.delivery.dispatch.domain;

/**
 * Delivery-coordination lifecycle. Names are persisted (saga_instances.status)
 * and must stay identical to the legacy Saga enum.
 */
public enum DispatchStatus {
    STARTED,
    DELIVERY_CREATED,
    FINDING_SHIPPER,
    OFFER_PERSISTING,
    SHIPPER_FOUND,
    OFFER_RETIRING,
    SHIPPER_ASSIGNED,
    PICKING_UP,
    DELIVERING,
    COMPLETED,
    COMPENSATING,
    FAILED,
    CANCELLED;

    public boolean isTerminal() {
        return this == COMPLETED || this == CANCELLED || this == FAILED;
    }
}
