package com.delivery.match.domain.single;

import java.math.BigDecimal;
import java.util.UUID;

/** Canonical command facts; validation deliberately retains the V1 contract and error order. */
public record FindCommand(UUID eventId, Long deliveryId, Long orderId, BigDecimal totalPrice,
                          String paymentMethod, Double pickupLat, Double pickupLng,
                          String restaurantName, String pickupAddress, String deliveryAddress) {
    public void validate() {
        if (eventId == null || deliveryId == null || deliveryId <= 0) {
            throw new IllegalArgumentException(
                    "Invalid FindShipperEvent: stable eventId and positive deliveryId are required");
        }
        if (orderId == null || orderId <= 0 || totalPrice == null || totalPrice.signum() <= 0
                || !"COD".equalsIgnoreCase(paymentMethod)) {
            throw new IllegalArgumentException(
                    "Invalid COD match contract: orderId, positive totalPrice and paymentMethod=COD are required");
        }
        if (!validVietnamCoordinate(pickupLat, pickupLng)) {
            throw new IllegalArgumentException(
                    "Invalid match contract: canonical Vietnam pickup coordinates are required");
        }
        if (!hasText(restaurantName) || !hasText(pickupAddress) || !hasText(deliveryAddress)) {
            throw new IllegalArgumentException(
                    "Invalid match contract: canonical restaurant and address text are required");
        }
    }

    private static boolean validVietnamCoordinate(Double latitude, Double longitude) {
        return latitude != null && longitude != null && Double.isFinite(latitude) && Double.isFinite(longitude)
                && latitude >= 8.0 && latitude <= 24.0 && longitude >= 102.0 && longitude <= 110.0;
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
