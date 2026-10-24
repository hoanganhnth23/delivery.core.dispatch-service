package com.delivery.saga_orchestrator_service.entity;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * ✅ Saga Instance — theo dõi trạng thái toàn bộ saga flow
 * Mỗi đơn hàng tạo 1 SagaInstance duy nhất
 */
@Entity
@Table(name = "saga_instances", indexes = {
    @Index(name = "idx_saga_order_id", columnList = "orderId"),
    @Index(name = "idx_saga_status", columnList = "status")
})
@Data
@NoArgsConstructor
public class SagaInstance {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Version
    private Long version;

    @Column(nullable = false)
    private String sagaType; // ORDER_CREATION, ORDER_CANCELLATION

    @Column(nullable = false, unique = true)
    private Long orderId;

    private Long deliveryId;
    private Long shipperId;

    @Column(nullable = false)
    private long orderStatusSequence;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private SagaStatus status;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private String payload; // JSON data từ event gốc

    @Column(nullable = false)
    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;
    private LocalDateTime completedAt;

    private LocalDateTime stateEnteredAt;
    private LocalDateTime stuckLastResentAt;
    @Column(nullable = false)
    private int stuckResendAttempts;

    public void setStatus(SagaStatus status) {
        if (this.status != status) {
            stateEnteredAt = LocalDateTime.now();
            stuckLastResentAt = null;
            stuckResendAttempts = 0;
        }
        this.status = status;
    }

    @OneToMany(mappedBy = "sagaInstance", cascade = CascadeType.ALL, orphanRemoval = true)
    @OrderBy("executedAt ASC")
    private List<SagaStep> steps = new ArrayList<>();

    public enum SagaStatus {
        STARTED,          // Saga vừa bắt đầu (order-created)
        DELIVERY_CREATED, // Delivery đã tạo
        FINDING_SHIPPER,  // Đang tìm shipper
        OFFER_PERSISTING, // Match found a candidate; Delivery has not confirmed its offer yet
        SHIPPER_FOUND,    // Tìm thấy shipper
        OFFER_RETIRING,   // Waiting for Delivery to fence an expired/abandoned offer session
        SHIPPER_ASSIGNED, // Shipper đã nhận đơn
        PICKING_UP,       // Shipper đang lấy hàng
        DELIVERING,       // Đang giao
        COMPLETED,        // Hoàn thành
        COMPENSATING,     // Đang rollback
        FAILED,           // Thất bại
        CANCELLED         // Bị huỷ
    }

    @PrePersist
    public void prePersist() {
        if (createdAt == null) createdAt = LocalDateTime.now();
        if (updatedAt == null) updatedAt = createdAt;
        if (stateEnteredAt == null) stateEnteredAt = updatedAt;
    }

    @PreUpdate
    public void preUpdate() {
        updatedAt = LocalDateTime.now();
    }

    /**
     * Thêm 1 step vào saga
     */
    public SagaStep addStep(String stepName, String eventType, String eventData) {
        SagaStep step = new SagaStep();
        step.setSagaInstance(this);
        step.setStepName(stepName);
        step.setEventType(eventType);
        step.setEventData(eventData);
        step.setExecutedAt(LocalDateTime.now());
        this.steps.add(step);
        return step;
    }
}
