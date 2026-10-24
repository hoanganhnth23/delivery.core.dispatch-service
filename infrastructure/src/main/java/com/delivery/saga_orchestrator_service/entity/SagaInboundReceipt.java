package com.delivery.saga_orchestrator_service.entity;

import java.time.LocalDateTime;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** Durable inbox identity for every Kafka event applied to a Saga aggregate. */
@Entity
@Table(name = "saga_inbound_receipts")
@Getter
@NoArgsConstructor
public class SagaInboundReceipt {
    @Id
    @Column(name = "event_id", nullable = false, updatable = false)
    private UUID eventId;
    @Column(name = "topic", nullable = false, updatable = false)
    private String topic;
    @Column(name = "order_id", nullable = false, updatable = false)
    private Long orderId;
    @Column(name = "payload_fingerprint", nullable = false, updatable = false, length = 64)
    private String payloadFingerprint;
    @Column(name = "received_at", nullable = false, updatable = false)
    private LocalDateTime receivedAt;

    public SagaInboundReceipt(UUID eventId, String topic, Long orderId, String payloadFingerprint) {
        this.eventId = eventId;
        this.topic = topic;
        this.orderId = orderId;
        this.payloadFingerprint = payloadFingerprint;
        this.receivedAt = LocalDateTime.now();
    }
}
