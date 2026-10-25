package com.delivery.dispatch.domain;

/** Decisions for Delivery-owned acceptance and Order-owned cancellation facts. */
public final class AssignmentPolicy {

    private AssignmentPolicy() {
    }

    public enum Acceptance { REPLAY, IGNORE_REJECTED_SHIPPER, IGNORE_STATE, ASSIGN }

    /**
     * @param assignedShipperId    shipper currently recorded on the case
     * @param assignmentRecorded   whether a SHIPPER_ASSIGNED fact exists
     * @param shipperPreviouslyRejected whether this shipper rejected during the case
     */
    public static Acceptance onAcceptance(long orderId, DispatchStatus status, long shipperId,
                                          Long assignedShipperId, boolean assignmentRecorded,
                                          boolean shipperPreviouslyRejected) {
        if (shipperId <= 0) {
            throw new IllegalArgumentException("shipperId must be positive");
        }
        if (status == DispatchStatus.SHIPPER_ASSIGNED || status == DispatchStatus.PICKING_UP
                || status == DispatchStatus.DELIVERING || status == DispatchStatus.COMPLETED) {
            if (assignedShipperId != null && assignedShipperId == shipperId && assignmentRecorded) {
                return Acceptance.REPLAY;
            }
            throw new IllegalStateException("Shipper acceptance conflicts with saga assignment for orderId="
                    + orderId);
        }
        boolean awaiting = awaitsShipperDecision(status);
        if (awaiting && shipperPreviouslyRejected) {
            // A delayed acceptance from a rejecting shipper must not resurrect it.
            return Acceptance.IGNORE_REJECTED_SHIPPER;
        }
        return awaiting ? Acceptance.ASSIGN : Acceptance.IGNORE_STATE;
    }

    /** Rejection or post-accept cancel-assignment is accepted only while awaiting or assigned. */
    public static boolean acceptsRejection(long orderId, DispatchStatus status, Long assignedShipperId,
                                           Long rejectedShipperId) {
        if (!awaitsShipperDecision(status) && status != DispatchStatus.SHIPPER_ASSIGNED) {
            return false;
        }
        if (status == DispatchStatus.SHIPPER_ASSIGNED
                && (assignedShipperId == null || !assignedShipperId.equals(rejectedShipperId))) {
            throw new IllegalStateException("Assigned shipper does not match rejected shipper for orderId=" + orderId);
        }
        return true;
    }

    /**
     * Delivery accepts or rejects only after committing WAIT_SHIPPER_CONFIRM, so a
     * decision arriving while the offer confirmation is still in flight
     * (OFFER_PERSISTING) or while its expiry is being retired (OFFER_RETIRING)
     * proves the offer existed and must not be discarded (delivery-matching.md C14).
     */
    private static boolean awaitsShipperDecision(DispatchStatus status) {
        return status == DispatchStatus.FINDING_SHIPPER || status == DispatchStatus.OFFER_PERSISTING
                || status == DispatchStatus.SHIPPER_FOUND || status == DispatchStatus.OFFER_RETIRING;
    }

    public enum Cancellation { REPLAY, IGNORE_FAILED, CANCEL_IMMEDIATELY, COMPENSATE_DELIVERY }

    /**
     * @param cancellationRecorded whether an ORDER_CANCELLED fact exists
     * @param sameCancellation     whether the recorded cancellation is the same JSON event
     * @param deliveryKnown        whether a delivery identity is recorded
     */
    public static Cancellation onOrderCancelled(long orderId, DispatchStatus status, boolean cancellationRecorded,
                                                boolean sameCancellation, boolean deliveryKnown) {
        if (status == DispatchStatus.CANCELLED
                || (status == DispatchStatus.COMPENSATING && cancellationRecorded)) {
            if (sameCancellation) {
                return Cancellation.REPLAY;
            }
            throw new IllegalStateException("Conflicting order-cancelled event for orderId=" + orderId);
        }
        if (status == DispatchStatus.COMPLETED) {
            throw new IllegalStateException("Cannot cancel completed Saga for orderId=" + orderId);
        }
        if (status == DispatchStatus.FAILED) {
            return Cancellation.IGNORE_FAILED;
        }
        // With a known Delivery, wait for its durable CANCELLED status before completing.
        return deliveryKnown ? Cancellation.COMPENSATE_DELIVERY : Cancellation.CANCEL_IMMEDIATELY;
    }

    /** Restaurant confirmation opens the matching gate only before matching starts. */
    public static boolean acceptsRestaurantConfirmation(DispatchStatus status, boolean alreadyConfirmed) {
        return (status == DispatchStatus.STARTED || status == DispatchStatus.DELIVERY_CREATED) && !alreadyConfirmed;
    }
}
