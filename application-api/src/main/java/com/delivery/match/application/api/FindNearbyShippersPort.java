package com.delivery.match.application.api;

import java.util.List;

/** Application boundary for querying the local matching geo projection. */
public interface FindNearbyShippersPort {

    FindNearbyShippersResult findNearbyShippers(FindNearbyShippersQuery query);

    record FindNearbyShippersQuery(double latitude, double longitude, double radiusKm, int maxShippers) {
    }

    record FindNearbyShippersResult(List<NearbyShipper> shippers) {
        public FindNearbyShippersResult {
            shippers = shippers == null ? List.of() : List.copyOf(shippers);
        }
    }

    record NearbyShipper(Long shipperId, double latitude, double longitude,
                         double distanceKm, boolean online, long completedDeliveries) {
    }
}
