package com.delivery.dispatch.domain;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/** Admission rules for a canonical find-shipper command rebuilt from case-owned facts. */
public final class MatchingCommandPolicy {

    private MatchingCommandPolicy() {
    }

    public static void requireCanonical(boolean hasOrderId, boolean hasDeliveryId,
                                        String paymentMethod, BigDecimal totalPrice) {
        if (!hasOrderId || !hasDeliveryId) {
            throw new IllegalArgumentException("Canonical matching payload is missing orderId/deliveryId");
        }
        if (paymentMethod == null || !"COD".equalsIgnoreCase(paymentMethod)) {
            throw new IllegalArgumentException("COD is the only supported MVP matching payment method");
        }
        if (totalPrice == null || totalPrice.signum() <= 0) {
            throw new IllegalArgumentException("Canonical COD totalPrice must be greater than zero");
        }
    }

    /** Matching deadline for the first generation; at least one minute after now. */
    public static LocalDateTime initialDeadline(LocalDateTime now, int findingShipperTimeoutMinutes) {
        return now.plusMinutes(Math.max(1, findingShipperTimeoutMinutes));
    }
}
