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

/**
 * Match-owned inbox plus the durable state needed to resume a command after a
 * broker/process crash. It deliberately does not make Redis GEO or offers a
 * relational source of truth.
 */
@Entity
@Table(name = "match_commands", indexes = {
        @Index(name = "idx_match_commands_delivery", columnList = "delivery_id, status"),
        @Index(name = "idx_match_commands_order", columnList = "order_id, created_at")
})
@Getter
@Setter
@NoArgsConstructor
public class MatchCommand {

    public enum Status {
        PENDING,
        CANDIDATE_STAGED,
        RESULT_STAGED,
        CANCELLED
    }

    @Id
    @Column(name = "event_id", nullable = false, updatable = false)
    private UUID eventId;

    @Column(nullable = false, updatable = false, length = 255)
    private String topic;

    @Column(name = "order_id", nullable = false, updatable = false)
    private Long orderId;

    @Column(name = "delivery_id", nullable = false, updatable = false)
    private Long deliveryId;

    /**
     * Saga-owned matching generation. It is intentionally distinct from the
     * Kafka command event ID so a stop command can target one matching attempt
     * even when its own outbox event has a different identity.
     */
    @Column(name = "matching_session_id", nullable = false, updatable = false)
    private UUID matchingSessionId;

    @Column(name = "dispatch_round_id")
    private UUID dispatchRoundId;

    @Column(name = "next_attempt_at")
    private LocalDateTime nextAttemptAt;

    @Column(name = "matching_deadline_at")
    private LocalDateTime matchingDeadlineAt;

    @Column(name = "payload", nullable = false, updatable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(name = "payload_fingerprint", nullable = false, updatable = false, length = 64)
    private String payloadFingerprint;

    /**
     * The first selected candidate is stored before the Redis reservation. A
     * replay therefore resumes the same candidate instead of selecting a new
     * shipper after a crash between reservation and result persistence.
     */
    @Column(name = "candidate_payload", columnDefinition = "TEXT")
    private String candidatePayload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private Status status;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @Column(name = "completed_at")
    private LocalDateTime completedAt;

    public MatchCommand(
            UUID eventId,
            String topic,
            Long orderId,
            Long deliveryId,
            UUID matchingSessionId,
            String payload,
            String payloadFingerprint) {
        LocalDateTime now = LocalDateTime.now();
        this.eventId = eventId;
        this.topic = topic;
        this.orderId = orderId;
        this.deliveryId = deliveryId;
        this.matchingSessionId = matchingSessionId;
        this.payload = payload;
        this.payloadFingerprint = payloadFingerprint;
        this.status = Status.PENDING;
        this.createdAt = now;
        this.updatedAt = now;
    }
}
