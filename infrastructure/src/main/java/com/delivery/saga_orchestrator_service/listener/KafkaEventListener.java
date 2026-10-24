package com.delivery.saga_orchestrator_service.listener;

import com.delivery.saga_orchestrator_service.service.SagaManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.kafka.support.Acknowledgment;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * ✅ Kafka Event Listener — Nhận events từ các service, delegate cho SagaManager
 * phát lệnh
 *
 * Topics nhận:
 * - order.created, order.cancelled (từ order-service)
 * - delivery.created.result (từ delivery-service, sau khi nhận lệnh saga)
 * - delivery.shipper-accepted (từ delivery-service, shipper bấm accept)
 * - delivery.status-updated (từ delivery-service, thay đổi status)
 * - shipper.found, shipper.not-found (từ match-service)
 */
@Slf4j
@Component
@RetryableTopic(
        attempts = "${app.kafka.retry.attempts:4}",
        backoff = @Backoff(delayExpression = "${app.kafka.retry.initial-delay-ms:1000}",
                multiplierExpression = "${app.kafka.retry.multiplier:2.0}",
                maxDelayExpression = "${app.kafka.retry.max-delay-ms:10000}"),
        exclude = IllegalArgumentException.class,
        kafkaTemplate = "retryKafkaTemplate",
        autoCreateTopics = "${app.kafka.retry.auto-create-topics:false}",
        // This service shares order.created, restaurant.order-confirmed and
        // delivery.status-updated with other consumer groups. Spring Kafka
        // requires an isolated retry/DLT topology per listener owner.
        retryTopicSuffix = "-retry-saga",
        dltTopicSuffix = ".saga.DLT")
public class KafkaEventListener {

    private final SagaManager sagaManager;
    private final ObjectMapper objectMapper;

    public KafkaEventListener(SagaManager sagaManager) {
        this.sagaManager = sagaManager;
        this.objectMapper = new ObjectMapper();
    }

    // ==================== ORDER EVENTS ====================

    @KafkaListener(topics = "${app.kafka.input-topics.order-created:order.created}")
    public void handleOrderCreated(String message, Acknowledgment ack) {
        try {
            JsonNode json = objectMapper.readTree(message);
            Long orderId = extractLong(json, "orderId");
            extractUuid(json, "eventId");
            log.info("📥 [Saga] order.created — orderId={}", orderId);

            sagaManager.handleOrderCreated(orderId, message);
            ack.acknowledge();

        } catch (Exception e) {
            log.error("💥 [Saga] Error processing order.created: {}", e.getMessage(), e);
            throw processingFailure("order.created", e);
        }
    }

    @KafkaListener(topics = "${app.kafka.input-topics.order-cancelled:order.cancelled}")
    public void handleOrderCancelled(String message, Acknowledgment ack) {
        try {
            JsonNode json = objectMapper.readTree(message);
            extractUuid(json, "eventId");
            Long orderId = extractLong(json, "orderId");
            log.info("📥 [Saga] order.cancelled — orderId={}", orderId);

            sagaManager.handleOrderCancelled(orderId, message);
            ack.acknowledge();

        } catch (Exception e) {
            log.error("💥 [Saga] Error processing order.cancelled: {}", e.getMessage(), e);
            throw processingFailure("order.cancelled", e);
        }
    }

    @KafkaListener(topics = "${app.kafka.input-topics.restaurant-confirmed:restaurant.order-confirmed}")
    public void handleRestaurantConfirmed(String message, Acknowledgment ack) {
        try {
            JsonNode json = objectMapper.readTree(message);
            extractUuid(json, "eventId");
            Long orderId = extractLong(json, "orderId");
            log.info("📥 [Saga] restaurant.order-confirmed — orderId={}", orderId);

            sagaManager.handleRestaurantConfirmed(orderId, message);
            ack.acknowledge();

        } catch (Exception e) {
            log.error("💥 [Saga] Error processing restaurant.order-confirmed: {}", e.getMessage(), e);
            throw processingFailure("restaurant.order-confirmed", e);
        }
    }

    // ==================== DELIVERY RESULT EVENTS ====================

    @KafkaListener(topics = "${app.kafka.input-topics.delivery-created:delivery.created.result}")
    public void handleDeliveryCreated(String message, Acknowledgment ack) {
        try {
            JsonNode json = objectMapper.readTree(message);
            extractUuid(json, "eventId");
            Long orderId = extractLong(json, "orderId");
            Long deliveryId = extractLong(json, "deliveryId");
            log.info("📥 [Saga] delivery.created.result — orderId={}, deliveryId={}", orderId, deliveryId);

            sagaManager.handleDeliveryCreated(orderId, deliveryId, message);
            ack.acknowledge();

        } catch (Exception e) {
            log.error("💥 [Saga] Error processing delivery.created.result: {}", e.getMessage(), e);
            throw processingFailure("delivery.created.result", e);
        }
    }

    @KafkaListener(topics = "${app.kafka.input-topics.shipper-accepted:delivery.shipper-accepted}")
    public void handleShipperAccepted(String message, Acknowledgment ack) {
        try {
            JsonNode json = objectMapper.readTree(message);
            extractUuid(json, "eventId");
            Long orderId = extractLong(json, "orderId");
            Long deliveryId = extractLong(json, "deliveryId");
            Long shipperId = extractLong(json, "shipperId");
            log.info("📥 [Saga] delivery.shipper-accepted — orderId={}, shipperId={}", orderId, shipperId);

            sagaManager.handleShipperAccepted(orderId, deliveryId, shipperId, message);
            ack.acknowledge();

        } catch (Exception e) {
            log.error("💥 [Saga] Error processing delivery.shipper-accepted: {}", e.getMessage(), e);
            throw processingFailure("delivery.shipper-accepted", e);
        }
    }

    @KafkaListener(topics = "${app.kafka.input-topics.offer-persisted:delivery.offer-persisted}")
    public void handleOfferPersisted(String message, Acknowledgment ack) {
        try {
            JsonNode json = objectMapper.readTree(message);
            extractUuid(json, "eventId");
            Long orderId = extractLong(json, "orderId");
            Long deliveryId = extractLong(json, "deliveryId");
            sagaManager.handleOfferPersisted(orderId, deliveryId, message);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("💥 [Saga] Error processing delivery.offer-persisted: {}", e.getMessage(), e);
            throw processingFailure("delivery.offer-persisted", e);
        }
    }

    @KafkaListener(topics = "${app.kafka.input-topics.offer-retired:delivery.offer-retired}")
    public void handleOfferRetired(String message, Acknowledgment ack) {
        try {
            JsonNode json = objectMapper.readTree(message);
            extractUuid(json, "eventId");
            Long orderId = extractLong(json, "orderId");
            Long deliveryId = extractLong(json, "deliveryId");
            sagaManager.handleOfferRetired(orderId, deliveryId, message);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("💥 [Saga] Error processing delivery.offer-retired: {}", e.getMessage(), e);
            throw processingFailure("delivery.offer-retired", e);
        }
    }

    @KafkaListener(topics = "${app.kafka.input-topics.delivery-status:delivery.status-updated}")
    public void handleDeliveryStatusUpdated(String message, Acknowledgment ack) {
        try {
            JsonNode json = objectMapper.readTree(message);
            extractUuid(json, "eventId");
            Long orderId = extractLong(json, "orderId");
            Long deliveryId = extractLong(json, "deliveryId");
            String newStatus = "";
            if (json.has("newStatus") && !json.get("newStatus").isNull()) {
                newStatus = json.get("newStatus").asText();
            } else if (json.has("status") && !json.get("status").isNull()) {
                newStatus = json.get("status").asText();
            }
            if (newStatus.isBlank()) {
                throw new IllegalArgumentException("delivery status is required");
            }
            log.info("📥 [Saga] delivery.status-updated — orderId={}, status={}", orderId, newStatus);

            sagaManager.handleDeliveryStatusUpdated(orderId, deliveryId, newStatus, message);
            ack.acknowledge();

        } catch (Exception e) {
            log.error("💥 [Saga] Error processing delivery.status-updated: {}", e.getMessage(), e);
            throw processingFailure("delivery.status-updated", e);
        }
    }

    /**
     * ✅ NEW: Shipper rejected → Saga re-triggers find shipper (re-assign)
     */
    @KafkaListener(topics = "${app.kafka.input-topics.shipper-rejected:delivery.shipper-rejected}")
    public void handleShipperRejected(String message, Acknowledgment ack) {
        try {
            JsonNode json = objectMapper.readTree(message);
            extractUuid(json, "eventId");
            Long orderId = extractLong(json, "orderId");
            Long deliveryId = extractLong(json, "deliveryId");
            Long rejectedShipperId = extractLong(json, "rejectedShipperId");
            log.info("📥 [Saga] delivery.shipper-rejected — orderId={}, rejectedShipperId={}", orderId, rejectedShipperId);

            sagaManager.handleShipperRejected(orderId, deliveryId, rejectedShipperId, message);
            ack.acknowledge();

        } catch (Exception e) {
            log.error("💥 [Saga] Error processing delivery.shipper-rejected: {}", e.getMessage(), e);
            throw processingFailure("delivery.shipper-rejected", e);
        }
    }

    // ==================== MATCH RESULT EVENTS ====================

    @KafkaListener(topics = "${app.kafka.input-topics.shipper-found:shipper.found}")
    public void handleShipperFound(String message, Acknowledgment ack) {
        try {
            JsonNode json = objectMapper.readTree(message);
            extractUuid(json, "eventId");
            Long orderId = extractLong(json, "orderId");
            Long deliveryId = extractLong(json, "deliveryId");
            log.info("📥 [Saga] shipper.found — orderId={}, deliveryId={}", orderId, deliveryId);

            sagaManager.handleShipperFound(orderId, deliveryId, message);
            ack.acknowledge();

        } catch (Exception e) {
            log.error("💥 [Saga] Error processing shipper.found: {}", e.getMessage(), e);
            throw processingFailure("shipper.found", e);
        }
    }

    @KafkaListener(topics = "${app.kafka.input-topics.shipper-not-found:shipper.not-found}")
    public void handleShipperNotFound(String message, Acknowledgment ack) {
        try {
            JsonNode json = objectMapper.readTree(message);
            extractUuid(json, "eventId");
            Long orderId = extractLong(json, "orderId");
            Long deliveryId = extractLong(json, "deliveryId");
            log.info("📥 [Saga] shipper.not-found — orderId={}, deliveryId={}", orderId, deliveryId);

            sagaManager.handleShipperNotFound(orderId, deliveryId, message);
            ack.acknowledge();

        } catch (Exception e) {
            log.error("💥 [Saga] Error processing shipper.not-found: {}", e.getMessage(), e);
            throw processingFailure("shipper.not-found", e);
        }
    }

    // ==================== FAILURE EVENTS ====================

    @KafkaListener(topics = "${app.kafka.input-topics.delivery-created-failed:delivery.created.failed}")
    public void handleDeliveryCreationFailed(String message, Acknowledgment ack) {
        try {
            JsonNode json = objectMapper.readTree(message);
            extractUuid(json, "eventId");
            Long orderId = extractLong(json, "orderId");
            String reason = json.has("reason") ? json.get("reason").asText() : "Unknown error";
            log.error("📥 [Saga] delivery.created.failed — orderId={}, reason={}", orderId, reason);

            sagaManager.handleDeliveryCreationFailed(orderId, reason, message);
            ack.acknowledge();

        } catch (Exception e) {
            log.error("💥 [Saga] Error processing delivery.created.failed: {}", e.getMessage(), e);
            throw processingFailure("delivery.created.failed", e);
        }
    }

    @KafkaListener(topics = "${app.kafka.input-topics.delivery-cancel-failed:delivery.cancel.failed}")
    public void handleDeliveryCancelFailed(String message, Acknowledgment ack) {
        try {
            JsonNode json = objectMapper.readTree(message);
            extractUuid(json, "eventId");
            Long orderId = extractLong(json, "orderId");
            String reason = json.has("reason") ? json.get("reason").asText() : "Unknown error";
            log.error("📥 [Saga] delivery.cancel.failed — orderId={}, reason={}", orderId, reason);

            sagaManager.handleStepFailed("DELIVERY_CANCEL", orderId, reason, message);
            ack.acknowledge();

        } catch (Exception e) {
            log.error("💥 [Saga] Error processing delivery.cancel.failed: {}", e.getMessage(), e);
            throw processingFailure("delivery.cancel.failed", e);
        }
    }

    // ==================== HELPER ====================

    private Long extractLong(JsonNode json, String field) {
        JsonNode node = json.get(field);
        if (node == null || node.isNull() || !node.canConvertToLong()) {
            throw new IllegalArgumentException(field + " is required and must be a number");
        }
        long value = node.asLong();
        if (value <= 0) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        return value;
    }

    private UUID extractUuid(JsonNode json, String field) {
        JsonNode node = json.get(field);
        if (node == null || node.isNull() || !node.isTextual()) {
            throw new IllegalArgumentException(field + " is required and must be a UUID");
        }
        try {
            return UUID.fromString(node.asText());
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException(field + " is required and must be a UUID", invalid);
        }
    }

    private RuntimeException processingFailure(String topic, Exception cause) {
        if (cause instanceof IllegalArgumentException poison) {
            return poison;
        }
        return new IllegalStateException("Failed to process " + topic, cause);
    }
}
