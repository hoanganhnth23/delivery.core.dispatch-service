package com.delivery.saga_orchestrator_service.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Durable staging for a valid cross-topic fact that arrives before its Saga
 * aggregate exists. The event is promoted into the normal Saga inbox before it
 * is applied, so normal replay/fingerprint rules remain the authority.
 */
@Entity
@Table(name = "saga_early_events", indexes = {
        @Index(name = "idx_saga_early_events_order_received", columnList = "order_id,received_at")
})
@Getter
@NoArgsConstructor
public class SagaEarlyEvent {
    @Id
    @Column(name = "event_id", nullable = false, updatable = false)
    private UUID eventId;
    @Column(name = "topic", nullable = false, updatable = false)
    private String topic;
    @Column(name = "order_id", nullable = false, updatable = false)
    private Long orderId;
    @Column(name = "payload", nullable = false, updatable = false, columnDefinition = "TEXT")
    private String payload;
    @Column(name = "payload_fingerprint", nullable = false, updatable = false, length = 64)
    private String payloadFingerprint;
    @Column(name = "received_at", nullable = false, updatable = false)
    private LocalDateTime receivedAt;

    public SagaEarlyEvent(UUID eventId, String topic, Long orderId, String payload, String payloadFingerprint) {
        this.eventId = eventId;
        this.topic = topic;
        this.orderId = orderId;
        this.payload = payload;
        this.payloadFingerprint = payloadFingerprint;
        this.receivedAt = LocalDateTime.now();
    }
}
