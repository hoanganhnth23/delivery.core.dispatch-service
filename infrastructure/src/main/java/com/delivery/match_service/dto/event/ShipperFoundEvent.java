package com.delivery.match_service.dto.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;
import java.math.BigDecimal;
import java.util.UUID;
import com.delivery.identity.contracts.SimulationContext;

/**
 * ✅ Event được bắn khi tìm được shipper thành công
 * Dùng cho cả delivery-service (cache waiting state) và notification-service (notify shipper)
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShipperFoundEvent {
    private String eventId;
    private Long deliveryId;
    private Long orderId;
    private List<ShipperMatchResult> availableShippers;
    private LocalDateTime foundAt;
    private Integer waitingTimeoutSeconds; // Thời gian chờ shipper nhận
    private String matchingSessionId;
    
    // ✅ Additional info for notification-service
    private String restaurantName;
    private String pickupAddress;
    private String deliveryAddress;
    private Double pickupLat;
    private Double pickupLng;
    private Double deliveryLat;
    private Double deliveryLng;
    private BigDecimal totalPrice;
    private String paymentMethod;
    private SimulationContext simulationContext;

    /** Additive batch contract. Null/false preserves the legacy single-offer flow. */
    private Boolean batchOffer;
    private UUID batchId;
    private List<BatchItem> batchItems;
    private List<UUID> codHoldIds;
    private Integer batchWave;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchItem {
        private Long deliveryId;
        private Long orderId;
        private Integer pickupSequence;
        private Integer dropoffSequence;
        private BigDecimal totalPrice;
        private UUID matchingSessionId;
    }
    
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ShipperMatchResult {
        private Long shipperId;
        private String shipperName;
        private String shipperPhone;
        private Double distanceKm;
        private Double latitude;
        private Double longitude;
        private Double rating;
        private Boolean isOnline;
    }
    
    // Constructor cho easy creation
    public ShipperFoundEvent(Long deliveryId, Long orderId, List<ShipperMatchResult> shippers) {
        this.deliveryId = deliveryId;
        this.orderId = orderId;
        this.availableShippers = shippers;
        this.foundAt = LocalDateTime.now();
        this.waitingTimeoutSeconds = com.delivery.match.domain.single.SingleOfferPolicy.WAITING_TIMEOUT_SECONDS; // Keep aligned with the Saga SHIPPER_FOUND timeout
        this.matchingSessionId = "delivery_" + deliveryId;
    }
}
