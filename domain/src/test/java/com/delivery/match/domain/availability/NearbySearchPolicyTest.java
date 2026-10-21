package com.delivery.match.domain.availability;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NearbySearchPolicyTest {

    @Test
    void acceptsBoundsAndReportsFirstViolationInOrder() {
        assertNull(NearbySearchPolicy.validationError(10, 106, 5, 10));
        assertNull(NearbySearchPolicy.validationError(-90, -180, 50, 100));
        assertEquals("Latitude phải trong khoảng -90 đến 90", NearbySearchPolicy.validationError(91, 999, 0, 0));
        assertEquals("Latitude phải trong khoảng -90 đến 90", NearbySearchPolicy.validationError(-91, 0, 5, 10));
        assertEquals("Longitude phải trong khoảng -180 đến 180", NearbySearchPolicy.validationError(0, 181, 0, 0));
        assertEquals("Longitude phải trong khoảng -180 đến 180", NearbySearchPolicy.validationError(0, -181, 5, 10));
        assertEquals("Bán kính phải từ 0.1 đến 50 km", NearbySearchPolicy.validationError(0, 0, 0, 0));
        assertEquals("Bán kính phải từ 0.1 đến 50 km", NearbySearchPolicy.validationError(0, 0, 50.1, 10));
        assertEquals("Số lượng shipper phải từ 1 đến 100", NearbySearchPolicy.validationError(0, 0, 5, 0));
        assertEquals("Số lượng shipper phải từ 1 đến 100", NearbySearchPolicy.validationError(0, 0, 5, 101));
    }
}
