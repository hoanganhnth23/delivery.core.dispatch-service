package com.delivery.match_service.dto.response;


import lombok.Getter;
import lombok.Setter;


/**
 * ✅ Response DTO cho shipper gần nhất
 * Theo Backend Instructions: Naming convention {Entity}Response
 */
@Getter
@Setter
public class NearbyShipperResponse {

    // Shipper identification
    private Long shipperId;      // ID của shipper
    private String shipperName;  // Tên shipper  
    private String shipperPhone; // SĐT shipper
    
    // Location information
    private double latitude;
    private double longitude;
    private double distanceKm; // Khoảng cách từ điểm giao hàng
    private boolean isOnline;
    private long completedDeliveries;
    private Double combinedScoreMinutes;

    // Metadata
    private String lastUpdated;

    // Constructors
    public NearbyShipperResponse() {
    }

    public NearbyShipperResponse(double latitude, double longitude,
            double distanceKm, boolean isOnline, String lastUpdated) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.distanceKm = distanceKm;
        this.isOnline = isOnline;
        this.lastUpdated = lastUpdated;
    }
    
    // ✅ Constructor with shipper info
    public NearbyShipperResponse(Long shipperId, String shipperName, String shipperPhone,
                               double latitude, double longitude, double distanceKm, 
                               boolean isOnline, String lastUpdated) {
        this.shipperId = shipperId;
        this.shipperName = shipperName;
        this.shipperPhone = shipperPhone;
        this.latitude = latitude;
        this.longitude = longitude;
        this.distanceKm = distanceKm;
        this.isOnline = isOnline;
        this.lastUpdated = lastUpdated;
    }

}
