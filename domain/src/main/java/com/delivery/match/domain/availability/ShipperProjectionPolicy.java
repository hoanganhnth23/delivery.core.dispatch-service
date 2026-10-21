package com.delivery.match.domain.availability;

import java.util.Locale;
import java.util.Set;
import java.util.UUID;

/**
 * Admission of shipper availability facts into Match's local projection:
 * location (online with coordinates / offline without), busy/available
 * status, and completed-delivery fairness counts. Redis ordering fences
 * (newer timestamp wins, tombstones) remain in the infrastructure scripts.
 */
public final class ShipperProjectionPolicy {

    /** Online replays older than this are ignored rather than resurrecting stale availability. */
    public static final long ONLINE_LOCATION_MAX_AGE_MILLIS = 300_000L;

    private static final Set<String> STATUSES = Set.of("BUSY", "AVAILABLE");

    private ShipperProjectionPolicy() {
    }

    public enum LocationDecision { APPLY_ONLINE, IGNORE_EXPIRED_ONLINE, APPLY_OFFLINE }

    public static LocationDecision onLocation(long shipperId, Double latitude, Double longitude,
                                              Boolean online, long timestampMillis, long nowMillis) {
        if (shipperId <= 0 || timestampMillis <= 0 || online == null) {
            throw new IllegalArgumentException("Invalid shipper location event");
        }
        if (!online) {
            return LocationDecision.APPLY_OFFLINE;
        }
        if (latitude == null || !Double.isFinite(latitude) || latitude < -90 || latitude > 90
                || longitude == null || !Double.isFinite(longitude) || longitude < -180 || longitude > 180) {
            throw new IllegalArgumentException("Online location event requires valid coordinates");
        }
        return timestampMillis < nowMillis - ONLINE_LOCATION_MAX_AGE_MILLIS
                ? LocationDecision.IGNORE_EXPIRED_ONLINE
                : LocationDecision.APPLY_ONLINE;
    }

    /**
     * Validates a Delivery-owned busy/available fact and returns its canonical
     * upper-case status.
     */
    public static String canonicalStatus(long shipperId, Long deliveryId, Long orderId, long timestampMillis,
                                         String eventId, String status) {
        if (shipperId <= 0 || deliveryId == null || deliveryId <= 0
                || orderId == null || orderId <= 0 || timestampMillis <= 0 || eventId == null) {
            throw new IllegalArgumentException(
                    "Stable eventId and positive shipper/delivery/order/timestamp are required");
        }
        UUID.fromString(eventId);
        String canonical = status == null ? null : status.toUpperCase(Locale.ROOT);
        // Set.of rejects null lookups; a missing status is unsupported, not a crash.
        if (canonical == null || !STATUSES.contains(canonical)) {
            throw new IllegalArgumentException("Unsupported shipper status: " + status);
        }
        return canonical;
    }

    /** Completed deliveries feed fairness ranking only for a positive shipper. */
    public static long completedDeliveryShipper(Object rawShipperId) {
        if (!(rawShipperId instanceof Number number) || number.longValue() <= 0) {
            throw new IllegalArgumentException("delivery.completed requires a positive shipperId");
        }
        return number.longValue();
    }
}
