package com.delivery.match.application;

import com.delivery.match.application.api.FindNearbyShippersPort;
import com.delivery.match.application.api.NearbyShipperGeoPort;
import com.delivery.match.domain.availability.NearbySearchPolicy;

import java.util.Objects;

/** Bounded nearby-shipper search; adapters map HTTP and the GEO store. */
public final class DefaultFindNearbyShippersUseCase implements FindNearbyShippersPort {

    private final NearbyShipperGeoPort geo;

    public DefaultFindNearbyShippersUseCase(NearbyShipperGeoPort geo) {
        this.geo = Objects.requireNonNull(geo, "geo");
    }

    @Override
    public FindNearbyShippersResult findNearbyShippers(FindNearbyShippersQuery query) {
        if (query == null) {
            throw new IllegalArgumentException("Request không được null");
        }
        String error = NearbySearchPolicy.validationError(
                query.latitude(), query.longitude(), query.radiusKm(), query.maxShippers());
        if (error != null) {
            throw new IllegalArgumentException(error);
        }
        return geo.nearby(query);
    }
}
