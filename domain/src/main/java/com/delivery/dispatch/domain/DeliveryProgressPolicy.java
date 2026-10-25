package com.delivery.dispatch.domain;

import java.util.Set;

/** Maps Delivery-owned status facts onto the coordination lifecycle. */
public final class DeliveryProgressPolicy {

    public static final String SHIPPER_NOT_FOUND = "SHIPPER_NOT_FOUND";

    private static final Set<DispatchStatus> CANCELLABLE = Set.of(
            DispatchStatus.STARTED,
            DispatchStatus.DELIVERY_CREATED,
            DispatchStatus.FINDING_SHIPPER,
            DispatchStatus.SHIPPER_FOUND,
            DispatchStatus.SHIPPER_ASSIGNED);

    private DeliveryProgressPolicy() {
    }

    /** The Delivery SHIPPER_NOT_FOUND status is an echo of an already applied Match outcome. */
    public static boolean isShipperNotFoundEcho(String deliveryStatus) {
        return SHIPPER_NOT_FOUND.equals(deliveryStatus);
    }

    public static DispatchStatus targetFor(String deliveryStatus) {
        if (deliveryStatus == null) {
            throw new IllegalArgumentException("Unsupported delivery status: null");
        }
        return switch (deliveryStatus) {
            case "PICKED_UP" -> DispatchStatus.PICKING_UP;
            case "DELIVERING" -> DispatchStatus.DELIVERING;
            case "DELIVERED" -> DispatchStatus.COMPLETED;
            case "CANCELLED" -> DispatchStatus.CANCELLED;
            default -> throw new IllegalArgumentException("Unsupported delivery status: " + deliveryStatus);
        };
    }

    public static void requireTransition(DispatchStatus current, DispatchStatus target, long orderId) {
        boolean valid = switch (target) {
            case PICKING_UP -> current == DispatchStatus.SHIPPER_ASSIGNED;
            case DELIVERING -> current == DispatchStatus.PICKING_UP;
            case COMPLETED -> current == DispatchStatus.DELIVERING;
            case CANCELLED -> CANCELLABLE.contains(current);
            default -> false;
        };
        if (!valid) {
            throw new IllegalStateException("Invalid saga delivery transition " + current + " -> "
                    + target + " for orderId=" + orderId);
        }
    }

    /**
     * A Delivery CANCELLED status confirms compensation when the order was
     * cancelled while compensating, or cleans up after a terminal failure/cancel.
     */
    public static boolean isCancellationConfirmation(DispatchStatus status, boolean orderCancelledRecorded) {
        if (status == DispatchStatus.COMPENSATING) {
            return orderCancelledRecorded;
        }
        return status == DispatchStatus.CANCELLED || status == DispatchStatus.FAILED;
    }
}
