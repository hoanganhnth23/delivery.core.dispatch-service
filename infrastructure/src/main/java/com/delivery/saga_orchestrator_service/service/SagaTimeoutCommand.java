package com.delivery.saga_orchestrator_service.service;

import com.delivery.saga_orchestrator_service.entity.SagaInstance;
import com.delivery.saga_orchestrator_service.entity.SagaInstance.SagaStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Immutable observation made by the Saga timeout scheduler.
 *
 * <p>The scheduler is intentionally not the state-transition authority. It
 * captures the exact version and state it observed, then {@link SagaManager}
 * re-locks the aggregate and applies the command only if that observation is
 * still current. The deterministic event ID also makes a repeated poll of the
 * same aggregate version an exact inbox replay instead of a second
 * compensation.</p>
 */
public record SagaTimeoutCommand(
        UUID eventId,
        Long orderId,
        Long deliveryId,
        SagaStatus expectedStatus,
        long expectedVersion,
        String observedUpdatedAt,
        String deadlineAt,
        String reason) {

    public static SagaTimeoutCommand forStage(
            SagaInstance saga,
            SagaStatus expectedStatus,
            Duration timeout,
            String reason) {
        if (timeout == null || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be non-negative");
        }
        LocalDateTime observedAt = requireObservedAt(saga);
        return create(saga, expectedStatus, observedAt.plus(timeout), reason);
    }

    /**
     * Offer expiry is computed from the persisted offer payload by the manager.
     * The snapshot timestamp is still carried for a precise stale-observation
     * fence and a deterministic identity.
     */
    public static SagaTimeoutCommand forShipperOffer(SagaInstance saga, String reason) {
        LocalDateTime observedAt = requireObservedAt(saga);
        return create(saga, SagaStatus.SHIPPER_FOUND, observedAt, reason);
    }

    private static SagaTimeoutCommand create(
            SagaInstance saga,
            SagaStatus expectedStatus,
            LocalDateTime deadlineAt,
            String reason) {
        if (saga == null || saga.getOrderId() == null || saga.getOrderId() <= 0) {
            throw new IllegalArgumentException("timeout candidate must have a positive orderId");
        }
        if (expectedStatus == null || deadlineAt == null || reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("timeout status, deadline and reason are required");
        }
        long version = saga.getVersion() == null ? 0L : saga.getVersion();
        String aggregateIdentity = saga.getId() == null
                ? "order:" + saga.getOrderId()
                : saga.getId().toString();
        UUID eventId = UUID.nameUUIDFromBytes(("saga-timeout:" + aggregateIdentity
                + ':' + expectedStatus.name() + ':' + version)
                .getBytes(StandardCharsets.UTF_8));
        LocalDateTime observedAt = requireObservedAt(saga);
        return new SagaTimeoutCommand(
                eventId,
                saga.getOrderId(),
                saga.getDeliveryId(),
                expectedStatus,
                version,
                observedAt.toString(),
                deadlineAt.toString(),
                reason);
    }

    public LocalDateTime observedAt() {
        return parse(observedUpdatedAt, "observedUpdatedAt");
    }

    public LocalDateTime deadline() {
        return parse(deadlineAt, "deadlineAt");
    }

    public String toJson(ObjectMapper objectMapper) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("eventId", eventId.toString());
        payload.put("eventType", "SAGA_TIMEOUT");
        payload.put("orderId", orderId);
        if (deliveryId == null) {
            payload.putNull("deliveryId");
        } else {
            payload.put("deliveryId", deliveryId);
        }
        payload.put("expectedStatus", expectedStatus.name());
        payload.put("expectedVersion", expectedVersion);
        payload.put("observedUpdatedAt", observedUpdatedAt);
        payload.put("deadlineAt", deadlineAt);
        payload.put("reason", reason);
        return payload.toString();
    }

    private static LocalDateTime requireObservedAt(SagaInstance saga) {
        if (saga == null || saga.getUpdatedAt() == null) {
            throw new IllegalArgumentException("timeout candidate must have updatedAt");
        }
        return saga.getUpdatedAt();
    }

    private static LocalDateTime parse(String value, String field) {
        try {
            return LocalDateTime.parse(value);
        } catch (Exception invalid) {
            throw new IllegalArgumentException(field + " must be an ISO-8601 local timestamp", invalid);
        }
    }
}
