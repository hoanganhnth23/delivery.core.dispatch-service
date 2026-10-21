package com.delivery.match_service.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Durable fence for one Saga matching generation. A tombstone can arrive on
 * the stop topic before its find command is consumed on the separate find
 * topic, so it cannot depend on a MatchCommand already existing.
 */
@Entity
@Table(name = "match_cancellation_tombstones",
        uniqueConstraints = @UniqueConstraint(
                name = "uk_match_cancellation_delivery_session",
                columnNames = {"delivery_id", "matching_session_id"}),
        indexes = {
                @Index(name = "idx_match_cancellation_delivery", columnList = "delivery_id, created_at"),
                @Index(name = "idx_match_cancellation_session", columnList = "matching_session_id")
        })
@Getter
@Setter
@NoArgsConstructor
public class MatchCancellationTombstone {

    public enum ProjectionStatus {
        PENDING,
        PROJECTED
    }

    @Id
    @Column(name = "event_id", nullable = false, updatable = false)
    private UUID eventId;

    @Column(name = "order_id", nullable = false, updatable = false)
    private Long orderId;

    @Column(name = "delivery_id", nullable = false, updatable = false)
    private Long deliveryId;

    @Column(name = "matching_session_id", nullable = false, updatable = false)
    private UUID matchingSessionId;

    @Column(name = "payload_fingerprint", nullable = false, updatable = false, length = 64)
    private String payloadFingerprint;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Enumerated(EnumType.STRING)
    @Column(name = "projection_status", nullable = false, length = 16)
    private ProjectionStatus projectionStatus;

    @Column(name = "projection_attempts", nullable = false)
    private int projectionAttempts;

    @Column(name = "next_projection_attempt_at", nullable = false)
    private LocalDateTime nextProjectionAttemptAt;

    @Column(name = "redis_projected_at")
    private LocalDateTime redisProjectedAt;

    @Column(name = "last_projection_error", length = 2000)
    private String lastProjectionError;

    public MatchCancellationTombstone(
            UUID eventId,
            Long orderId,
            Long deliveryId,
            UUID matchingSessionId,
            String payloadFingerprint) {
        this.eventId = eventId;
        this.orderId = orderId;
        this.deliveryId = deliveryId;
        this.matchingSessionId = matchingSessionId;
        this.payloadFingerprint = payloadFingerprint;
        LocalDateTime now = LocalDateTime.now();
        this.createdAt = now;
        this.projectionStatus = ProjectionStatus.PENDING;
        this.projectionAttempts = 0;
        this.nextProjectionAttemptAt = now;
    }
}
