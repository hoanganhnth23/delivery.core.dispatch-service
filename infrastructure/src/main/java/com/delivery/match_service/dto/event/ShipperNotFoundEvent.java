package com.delivery.match_service.dto.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import com.delivery.identity.contracts.SimulationContext;

/**
 * ✅ Event được bắn khi không tìm được shipper sau nhiều lần retry
 * Cho phép delivery-service và order-service cập nhật trạng thái
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShipperNotFoundEvent {
    private String eventId;
    private Long deliveryId;
    private Long orderId;
    private String matchingSessionId;
    private String reason;
    private LocalDateTime occurredAt;
    private Integer retryAttempts;
    private Double searchRadius;
    private Double pickupLat;
    private Double pickupLng;
    private Double deliveryLat;
    private Double deliveryLng;
    private SimulationContext simulationContext;
    
    // Constructor cho easy creation
    public ShipperNotFoundEvent(Long deliveryId, Long orderId, Integer retryAttempts) {
        this.deliveryId = deliveryId;
        this.orderId = orderId;
        this.retryAttempts = retryAttempts;
        this.reason = "No available shippers found after " + retryAttempts + " attempts";
        this.occurredAt = LocalDateTime.now();
    }
}
