package com.delivery.dispatch.domain;

import java.time.LocalDateTime;

/** The single active offer window derived from the persisted shipper.found fact. */
public record ShipperOffer(long shipperId, LocalDateTime foundAt, Integer waitingTimeoutSeconds) {

    public static final int MAX_TIMEOUT_SECONDS = 180;

    public int timeoutSeconds() {
        return waitingTimeoutSeconds == null
                ? MAX_TIMEOUT_SECONDS
                : Math.max(1, Math.min(waitingTimeoutSeconds, MAX_TIMEOUT_SECONDS));
    }

    public LocalDateTime expiresAt() {
        return foundAt.plusSeconds(timeoutSeconds());
    }

    /** True once the offer deadline has elapsed; an unknown start time is treated as due. */
    public static boolean isDue(LocalDateTime foundAt, Integer waitingTimeoutSeconds, LocalDateTime now) {
        if (foundAt == null) {
            return true;
        }
        return !new ShipperOffer(0, foundAt, waitingTimeoutSeconds).expiresAt().isAfter(now);
    }
}
