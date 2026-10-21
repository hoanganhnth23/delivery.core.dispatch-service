package com.delivery.match_service.dto.response;

import java.util.List;

/**
 * ✅ Response wrapper từ Tracking Service
 * Match với BaseResponse format của tracking service
 */
public class TrackingServiceResponse {
    
    private int status;
    private String message;
    private List<NearbyShipperResponse> data;
    
    // Constructors
    public TrackingServiceResponse() {}
    
    public TrackingServiceResponse(int status, String message, List<NearbyShipperResponse> data) {
        this.status = status;
        this.message = message;
        this.data = data;
    }
    
    // Getters và Setters
    public int getStatus() {
        return status;
    }
    
    public void setStatus(int status) {
        this.status = status;
    }
    
    public String getMessage() {
        return message;
    }
    
    public void setMessage(String message) {
        this.message = message;
    }
    
    public List<NearbyShipperResponse> getData() {
        return data;
    }
    
    public void setData(List<NearbyShipperResponse> data) {
        this.data = data;
    }
}
