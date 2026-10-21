package com.delivery.match.domain.availability;

import com.delivery.match.domain.availability.ShipperProjectionPolicy.LocationDecision;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ShipperProjectionPolicyTest {

    private static final long NOW = 1_800_000_000_000L;

    @Test
    void onlineLocationNeedsValidCoordinatesAndFreshness() {
        assertEquals(LocationDecision.APPLY_ONLINE,
                ShipperProjectionPolicy.onLocation(1, 10.7, 106.7, true, NOW, NOW));
        assertEquals(LocationDecision.APPLY_ONLINE,
                ShipperProjectionPolicy.onLocation(1, 10.7, 106.7, true, NOW - 300_000L, NOW));
        assertEquals(LocationDecision.IGNORE_EXPIRED_ONLINE,
                ShipperProjectionPolicy.onLocation(1, 10.7, 106.7, true, NOW - 300_001L, NOW));
        String coordinates = "Online location event requires valid coordinates";
        for (Double[] invalid : new Double[][] {{null, 1.0}, {1.0, null}, {Double.NaN, 1.0}, {1.0, Double.NaN},
                {-91.0, 1.0}, {91.0, 1.0}, {1.0, -181.0}, {1.0, 181.0}}) {
            assertEquals(coordinates, assertThrows(IllegalArgumentException.class,
                    () -> ShipperProjectionPolicy.onLocation(1, invalid[0], invalid[1], true, NOW, NOW)).getMessage());
        }
    }

    @Test
    void offlineNeedsNoCoordinatesButValidIdentity() {
        assertEquals(LocationDecision.APPLY_OFFLINE,
                ShipperProjectionPolicy.onLocation(1, null, null, false, 1, NOW));
        String invalid = "Invalid shipper location event";
        assertEquals(invalid, assertThrows(IllegalArgumentException.class,
                () -> ShipperProjectionPolicy.onLocation(0, null, null, false, 1, NOW)).getMessage());
        assertThrows(IllegalArgumentException.class, () -> ShipperProjectionPolicy.onLocation(1, null, null, false, 0, NOW));
        assertThrows(IllegalArgumentException.class, () -> ShipperProjectionPolicy.onLocation(1, null, null, null, 1, NOW));
    }

    @Test
    void statusFactIsCanonicalizedAndFullyIdentified() {
        String id = UUID.randomUUID().toString();
        assertEquals("BUSY", ShipperProjectionPolicy.canonicalStatus(1, 2L, 3L, 4, id, "busy"));
        assertEquals("AVAILABLE", ShipperProjectionPolicy.canonicalStatus(1, 2L, 3L, 4, id, "Available"));
        String identity = "Stable eventId and positive shipper/delivery/order/timestamp are required";
        assertEquals(identity, assertThrows(IllegalArgumentException.class,
                () -> ShipperProjectionPolicy.canonicalStatus(0, 2L, 3L, 4, id, "BUSY")).getMessage());
        assertThrows(IllegalArgumentException.class, () -> ShipperProjectionPolicy.canonicalStatus(1, null, 3L, 4, id, "BUSY"));
        assertThrows(IllegalArgumentException.class, () -> ShipperProjectionPolicy.canonicalStatus(1, 0L, 3L, 4, id, "BUSY"));
        assertThrows(IllegalArgumentException.class, () -> ShipperProjectionPolicy.canonicalStatus(1, 2L, null, 4, id, "BUSY"));
        assertThrows(IllegalArgumentException.class, () -> ShipperProjectionPolicy.canonicalStatus(1, 2L, 0L, 4, id, "BUSY"));
        assertThrows(IllegalArgumentException.class, () -> ShipperProjectionPolicy.canonicalStatus(1, 2L, 3L, 0, id, "BUSY"));
        assertThrows(IllegalArgumentException.class, () -> ShipperProjectionPolicy.canonicalStatus(1, 2L, 3L, 4, null, "BUSY"));
        assertThrows(IllegalArgumentException.class, () -> ShipperProjectionPolicy.canonicalStatus(1, 2L, 3L, 4, "not-a-uuid", "BUSY"));
        assertEquals("Unsupported shipper status: OFFLINE", assertThrows(IllegalArgumentException.class,
                () -> ShipperProjectionPolicy.canonicalStatus(1, 2L, 3L, 4, id, "OFFLINE")).getMessage());
        assertThrows(IllegalArgumentException.class, () -> ShipperProjectionPolicy.canonicalStatus(1, 2L, 3L, 4, id, null));
    }

    @Test
    void completedDeliveryRequiresPositiveNumericShipper() {
        assertEquals(7L, ShipperProjectionPolicy.completedDeliveryShipper(7));
        assertThrows(IllegalArgumentException.class, () -> ShipperProjectionPolicy.completedDeliveryShipper(0));
        assertThrows(IllegalArgumentException.class, () -> ShipperProjectionPolicy.completedDeliveryShipper("7"));
        assertThrows(IllegalArgumentException.class, () -> ShipperProjectionPolicy.completedDeliveryShipper(null));
    }
}
