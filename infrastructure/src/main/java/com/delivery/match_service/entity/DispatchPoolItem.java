package com.delivery.match_service.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.UUID;
import java.math.BigDecimal;

@Entity
@Table(name = "dispatch_pool_items", indexes = {
        @Index(name = "idx_dispatch_pool_ready", columnList = "state,eligible_at,matching_deadline_at"),
        @Index(name = "idx_dispatch_pool_zone", columnList = "pickup_h3_cell,state,eligible_at")
})
@Getter
@Setter
@NoArgsConstructor
public class DispatchPoolItem {

    public enum State {
        WAITING, CLAIMED, ASSIGNED, REQUEUED, EXPIRED, CANCELLED
    }

    @Id
    @Column(name = "pool_item_id", nullable = false, updatable = false)
    private UUID poolItemId;

    @Column(name = "order_id", nullable = false, updatable = false)
    private Long orderId;

    @Column(name = "delivery_id", nullable = false, updatable = false)
    private Long deliveryId;

    @Column(name = "matching_session_id", nullable = false, updatable = false)
    private UUID matchingSessionId;

    @Column(name = "pickup_h3_cell", length = 32)
    private String pickupH3Cell;

    @Column(name = "pickup_lat")
    private Double pickupLat;

    @Column(name = "pickup_lng")
    private Double pickupLng;

    @Column(name = "delivery_lat")
    private Double deliveryLat;

    @Column(name = "delivery_lng")
    private Double deliveryLng;

    @Column(name = "total_price", precision = 12, scale = 2)
    private BigDecimal totalPrice;

    @Column(name = "payment_method", length = 32)
    private String paymentMethod;

    @Column(name = "wave_number", nullable = false)
    private int waveNumber;

    @Column(name = "eligible_at", nullable = false)
    private LocalDateTime eligibleAt;

    @Column(name = "matching_deadline_at", nullable = false)
    private LocalDateTime matchingDeadlineAt;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 24)
    private State state = State.WAITING;

    @Column(name = "claimed_round_id")
    private UUID claimedRoundId;

    @Column(nullable = false)
    private long version;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;
}
