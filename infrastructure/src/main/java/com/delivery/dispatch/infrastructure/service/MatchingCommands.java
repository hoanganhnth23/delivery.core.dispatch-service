package com.delivery.dispatch.infrastructure.service;

import com.delivery.dispatch.domain.MatchingCommandAssembly;
import com.delivery.dispatch.domain.MatchingCommandPolicy;
import com.delivery.dispatch.domain.MatchingRetrySettings;
import com.delivery.dispatch.domain.MatchingSession;
import com.delivery.dispatch.infrastructure.entity.SagaInstance;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;

/**
 * Builds and sends Match commands for one dispatch case.
 *
 * <p>Every find-shipper command is rebuilt from case-owned facts (order money,
 * delivery coordinates, first-generation deadline) and carries a new
 * {@code matchingSessionId}. That generation id fences stale Match results and
 * scopes stop-matching to exactly one attempt.
 */
@Slf4j
final class MatchingCommands {

    private final ObjectMapper objectMapper;
    private final DispatchCommandOutbox outbox;
    private final BooleanSupplier batchClientCapabilityEnabled;

    MatchingCommands(ObjectMapper objectMapper, DispatchCommandOutbox outbox,
                     BooleanSupplier batchClientCapabilityEnabled) {
        this.objectMapper = objectMapper;
        this.outbox = outbox;
        this.batchClientCapabilityEnabled = batchClientCapabilityEnabled;
    }

    /**
     * Every matching attempt is rebuilt from Saga-owned canonical state. Rejection
     * and timeout events are control signals, not authorities for price/payment or
     * delivery coordinates.
     */
    ObjectNode buildFindShipperPayload(SagaInstance saga, String attemptEvent) throws Exception {
        JsonNode parsedAttempt = objectMapper.readTree(attemptEvent);
        if (!(parsedAttempt instanceof ObjectNode)) {
            throw new IllegalArgumentException("Find-shipper payload must be a JSON object");
        }
        JsonCaseHistory history = new JsonCaseHistory(saga, objectMapper);
        String deliveryData = history.latest("DELIVERY_CREATED");
        String matchingStartData = history.latest("MATCHING_STARTED");
        Map<String, Object> fields = MatchingCommandAssembly.assemble(
                facts(parsedAttempt),
                deliveryData == null ? null : facts(objectMapper.readTree(deliveryData)),
                matchingStartData == null ? null : facts(objectMapper.readTree(matchingStartData)),
                facts(objectMapper.readTree(saga.getPayload())),
                batchClientCapabilityEnabled.getAsBoolean());
        ObjectNode payload = objectMapper.createObjectNode();
        fields.forEach((field, value) -> {
            if (value instanceof JsonNode node) {
                payload.set(field, node);
            } else {
                payload.put(field, (Boolean) value);
            }
        });

        MatchingCommandPolicy.requireCanonical(payload.hasNonNull("orderId"), payload.hasNonNull("deliveryId"),
                payload.hasNonNull("paymentMethod") ? payload.get("paymentMethod").asText() : null,
                payload.hasNonNull("totalPrice") ? payload.get("totalPrice").decimalValue() : null);
        return payload;
    }

    /**
     * Every Find command carries a Saga-owned generation that is independent
     * of the outbox event ID. The persisted MATCHING_STARTED step is the
     * authoritative target for later stop-matching and stale-result fences.
     */
    String sendFindShipper(SagaInstance saga, Long orderId, ObjectNode payload) throws Exception {
        UUID matchingSessionId = nextSession(saga);
        payload.put("matchingSessionId", matchingSessionId.toString());
        String command = objectMapper.writeValueAsString(payload);
        outbox.send(DispatchCoordinator.CMD_FIND_SHIPPER, orderId.toString(), command);
        saga.addStep("MATCHING_STARTED", DispatchCoordinator.CMD_FIND_SHIPPER, command);
        return command;
    }

    /** Session of the latest generation; null for a case that predates generation-aware matching. */
    UUID currentSession(SagaInstance saga) {
        // Pre-contract cases yield null so no broad stop can cancel a later rematch.
        return new JsonCaseHistory(saga, objectMapper).currentMatchingSession();
    }

    boolean isCurrentResult(SagaInstance saga, String rawEvent) {
        UUID expected = currentSession(saga);
        if (expected == null) {
            // A Saga begun before the generation contract has no safe expected
            // value. Preserve its in-flight compatibility during rollout; new
            // matching attempts always persist the explicit session above.
            return true;
        }
        try {
            JsonNode result = objectMapper.readTree(rawEvent);
            if (!result.hasNonNull("matchingSessionId")) {
                throw new IllegalArgumentException(
                        "Match result matchingSessionId is required for a generation-aware Saga");
            }
            return MatchingSession.isCurrent(expected, UUID.fromString(result.get("matchingSessionId").asText()));
        } catch (IllegalArgumentException exception) {
            throw exception;
        } catch (Exception malformed) {
            throw new IllegalArgumentException("Match result matchingSessionId is malformed", malformed);
        }
    }

    void sendStopMatching(SagaInstance saga, Long orderId, String causeEvent) {
        UUID matchingSessionId = currentSession(saga);
        if (matchingSessionId == null) {
            log.info("[Saga] No generation-scoped Match command exists for orderId={}; skipping stop-matching",
                    orderId);
            return;
        }
        if (saga.getDeliveryId() == null || saga.getDeliveryId() <= 0) {
            throw new IllegalStateException(
                    "Cannot stop matching generation without a persisted deliveryId for orderId=" + orderId);
        }
        try {
            ObjectNode stop = objectMapper.createObjectNode();
            stop.put("orderId", orderId);
            stop.put("deliveryId", saga.getDeliveryId());
            stop.put("matchingSessionId", matchingSessionId.toString());
            JsonNode parsedCause = objectMapper.readTree(causeEvent);
            if (parsedCause.hasNonNull("eventId")) {
                stop.put("causeEventId", parsedCause.get("eventId").asText());
            }
            outbox.send(DispatchCoordinator.CMD_STOP_MATCHING, orderId.toString(), objectMapper.writeValueAsString(stop));
        } catch (DispatchCommandPublishException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot build generation-scoped stop-matching command", exception);
        }
    }

    static void putRetrySettings(ObjectNode payload, MatchingRetrySettings settings) {
        payload.put("maxRetryAttempts", settings.maxRetryAttempts());
        payload.put("initialDelaySeconds", settings.initialDelaySeconds());
        payload.put("maxDelaySeconds", settings.maxDelaySeconds());
        payload.put("backoffMultiplier", settings.backoffMultiplier());
    }

    private UUID nextSession(SagaInstance saga) {
        long started = new JsonCaseHistory(saga, objectMapper).count("MATCHING_STARTED");
        return MatchingSession.next(MatchingSession.caseIdentity(saga.getId(), saga.getOrderId()), started);
    }

    /** JSON view used by the domain assembly; a non-object source contributes no facts. */
    private static MatchingCommandAssembly.Facts facts(JsonNode source) {
        if (source == null || !source.isObject()) return null;
        return new MatchingCommandAssembly.Facts() {
            @Override public boolean hasNonNull(String field) { return source.hasNonNull(field); }
            @Override public Object get(String field) { return source.get(field); }
        };
    }
}
