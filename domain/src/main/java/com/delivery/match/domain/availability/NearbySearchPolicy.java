package com.delivery.match.domain.availability;

/** Bounds of an operator nearby-shipper search against the local GEO projection. */
public final class NearbySearchPolicy {

    public static final double MAX_RADIUS_KM = 50;
    public static final int MAX_SHIPPERS = 100;

    private NearbySearchPolicy() {
    }

    /** @return the user-facing validation message, or null when the search is valid */
    public static String validationError(double latitude, double longitude, double radiusKm, int maxShippers) {
        if (latitude < -90 || latitude > 90) {
            return "Latitude phải trong khoảng -90 đến 90";
        }
        if (longitude < -180 || longitude > 180) {
            return "Longitude phải trong khoảng -180 đến 180";
        }
        if (radiusKm <= 0 || radiusKm > MAX_RADIUS_KM) {
            return "Bán kính phải từ 0.1 đến 50 km";
        }
        if (maxShippers <= 0 || maxShippers > MAX_SHIPPERS) {
            return "Số lượng shipper phải từ 1 đến 100";
        }
        return null;
    }
}
