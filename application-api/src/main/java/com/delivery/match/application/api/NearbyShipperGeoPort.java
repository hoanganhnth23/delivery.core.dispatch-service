package com.delivery.match.application.api;

import com.delivery.match.application.api.FindNearbyShippersPort.FindNearbyShippersQuery;
import com.delivery.match.application.api.FindNearbyShippersPort.FindNearbyShippersResult;

/** Read access to the local GEO availability projection. */
@FunctionalInterface
public interface NearbyShipperGeoPort {

    FindNearbyShippersResult nearby(FindNearbyShippersQuery query);
}
