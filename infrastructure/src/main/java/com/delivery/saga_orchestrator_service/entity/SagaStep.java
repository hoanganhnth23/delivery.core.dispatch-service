package com.delivery.saga_orchestrator_service.entity;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * ✅ Saga Step — lịch sử từng bước trong saga
 * Mỗi Kafka event nhận được = 1 step record
 */
@Entity
@Table(name = "saga_steps")
@Data
@NoArgsConstructor
public class SagaStep {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "saga_instance_id", nullable = false)
    private SagaInstance sagaInstance;

    @Column(nullable = false)
    private String stepName; // ORDER_CREATED, DELIVERY_CREATED, SHIPPER_FOUND, etc.

    @Column(nullable = false)
    private String eventType; // Kafka topic name

    @Column(columnDefinition = "text")
    private String eventData; // Raw JSON of the event

    @Column(nullable = false)
    private LocalDateTime executedAt;

    @PrePersist
    public void prePersist() {
        if (executedAt == null) executedAt = LocalDateTime.now();
    }
}
