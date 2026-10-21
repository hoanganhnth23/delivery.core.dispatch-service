package com.delivery.match_service.dto.request;

/**
 * ✅ Request DTO để tìm shipper gần nhất
 * Theo Backend Instructions: Naming convention {Action}{Entity}Request
 */
public class FindNearbyShippersRequest {
    
    private double latitude;      // Vĩ độ điểm giao hàng
    private double longitude;     // Kinh độ điểm giao hàng
    private double radiusKm;      // Bán kính tìm kiếm (km)
    private int maxShippers;      // Số lượng shipper tối đa trả về
    
    // Constructors
    public FindNearbyShippersRequest() {}
    
    public FindNearbyShippersRequest(double latitude, double longitude, double radiusKm, int maxShippers) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.radiusKm = radiusKm;
        this.maxShippers = maxShippers;
    }
    
    // Getters và Setters
    public double getLatitude() {
        return latitude;
    }
    
    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }
    
    public double getLongitude() {
        return longitude;
    }
    
    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }
    
    public double getRadiusKm() {
        return radiusKm;
    }
    
    public void setRadiusKm(double radiusKm) {
        this.radiusKm = radiusKm;
    }
    
    public int getMaxShippers() {
        return maxShippers;
    }
    
    public void setMaxShippers(int maxShippers) {
        this.maxShippers = maxShippers;
    }
}
