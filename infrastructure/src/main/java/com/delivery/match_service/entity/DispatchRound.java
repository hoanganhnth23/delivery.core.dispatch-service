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

@Entity
@Table(name = "dispatch_rounds", indexes = {
        @Index(name = "idx_dispatch_rounds_zone_cutoff", columnList = "h3_zone,state,cutoff_at")
})
@Getter
@Setter
@NoArgsConstructor
public class DispatchRound {

    public enum State {
        OPEN, RUNNING, COMMITTED, REQUEUED, EXPIRED, CANCELLED
    }

    @Id
    @Column(name = "dispatch_round_id", nullable = false, updatable = false)
    private UUID dispatchRoundId;

    @Column(name = "h3_zone", nullable = false, length = 32)
    private String h3Zone;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 24)
    private State state = State.OPEN;

    @Column(name = "opened_at", nullable = false)
    private LocalDateTime openedAt;

    @Column(name = "cutoff_at", nullable = false)
    private LocalDateTime cutoffAt;

    @Column(name = "closed_at")
    private LocalDateTime closedAt;

    @Column(name = "order_count", nullable = false)
    private int orderCount;

    @Column(name = "shipper_count", nullable = false)
    private int shipperCount;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;
}
