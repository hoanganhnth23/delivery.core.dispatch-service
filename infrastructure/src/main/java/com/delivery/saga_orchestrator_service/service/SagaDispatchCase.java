package com.delivery.saga_orchestrator_service.service;

import com.delivery.dispatch.application.api.DispatchCase;
import com.delivery.dispatch.domain.CaseHistory;
import com.delivery.dispatch.domain.DispatchStatus;
import com.delivery.saga_orchestrator_service.entity.SagaInstance;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;

/** {@link DispatchCase} view of the legacy managed saga_instances row. */
final class SagaDispatchCase implements DispatchCase {

    private final SagaInstance saga;
    private final ObjectMapper objectMapper;

    SagaDispatchCase(SagaInstance saga, ObjectMapper objectMapper) {
        this.saga = saga;
        this.objectMapper = objectMapper;
    }

    SagaInstance saga() {
        return saga;
    }

    @Override
    public long orderId() {
        return saga.getOrderId();
    }

    @Override
    public Long deliveryId() {
        return saga.getDeliveryId();
    }

    @Override
    public void attachDelivery(Long deliveryId) {
        saga.setDeliveryId(deliveryId);
    }

    @Override
    public void clearCompletion() {
        saga.setCompletedAt(null);
    }

    @Override
    public DispatchStatus status() {
        return DispatchStatus.valueOf(saga.getStatus().name());
    }

    @Override
    public void transitionTo(DispatchStatus status) {
        saga.setStatus(SagaInstance.SagaStatus.valueOf(status.name()));
    }

    @Override
    public Long assignedShipper() {
        return saga.getShipperId();
    }

    @Override
    public void assignShipper(Long shipperId) {
        saga.setShipperId(shipperId);
    }

    @Override
    public void markCompleted() {
        saga.setCompletedAt(LocalDateTime.now());
    }

    @Override
    public CaseHistory history() {
        return new JsonCaseHistory(saga, objectMapper);
    }

    @Override
    public void record(String stepName, String eventType, String eventData) {
        saga.addStep(stepName, eventType, eventData);
    }
}
