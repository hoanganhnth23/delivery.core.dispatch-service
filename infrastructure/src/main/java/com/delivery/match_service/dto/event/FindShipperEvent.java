package com.delivery.match_service.dto.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.math.BigDecimal;
import java.util.UUID;
import com.delivery.identity.contracts.SimulationContext;

/**
 * ✅ Event nhận từ Delivery Service để tự động tìm shipper theo AI Instructions
 * Match với FindShipperEvent từ Delivery Service
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FindShipperEvent {

    private UUID eventId;
    
    private Long deliveryId;
    private Long orderId;
    
    // Restaurant information
    private String restaurantName;   // Tên nhà hàng
    private String pickupAddress;
    private Double pickupLat;
    private Double pickupLng;
    
    // Delivery information
    private String deliveryAddress;
    private Double deliveryLat;
    private Double deliveryLng;
    private LocalDateTime estimatedDeliveryTime;
    private String notes;
    private LocalDateTime createdAt;

    // Canonical COD eligibility data copied from order.created by Saga.
    private BigDecimal totalPrice;
    private String paymentMethod;
    
    // Event metadata
    private String eventType;
    private LocalDateTime timestamp;
    private LocalDateTime occurredAt;
    
    // Saga Match Configuration (Syncing timeout & retry from Orchestrator)
    private Integer maxRetryAttempts;
    private Integer initialDelaySeconds;
    private Integer maxDelaySeconds;
    private Double backoffMultiplier;
    /** Absolute Saga-owned cutoff; Match must never continue retrying past it. */
    private LocalDateTime matchingDeadlineAt;

    /**
     * Saga-owned matching generation. Unlike the Kafka command event ID it
     * remains stable as the target of stop-matching and result fencing.
     */
    private UUID matchingSessionId;
    
    // ✅ NEW: Excluded shipper IDs (shippers who already rejected this order)
    private java.util.List<Long> excludedShipperIds;

    /** Client capability negotiated by the order/delivery command producer. */
    private Boolean batchOfferEnabled;
    private Integer batchWave;
    private SimulationContext simulationContext;
}
