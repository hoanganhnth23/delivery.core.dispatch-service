package com.delivery.dispatch.infrastructure.service;

import com.delivery.dispatch.domain.CaseHistory;
import com.delivery.dispatch.domain.MatchingRetrySettings;
import com.delivery.dispatch.domain.RematchPolicy;
import com.delivery.dispatch.domain.ShipperOffer;
import com.delivery.dispatch.infrastructure.entity.SagaInstance;
import com.delivery.dispatch.infrastructure.entity.SagaInstance.SagaStatus;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Expiry of the single active shipper offer.
 *
 * <p>When the offer deadline passes, the rematch command is prepared and
 * persisted but not sent. The case waits in OFFER_RETIRING until Delivery
 * acknowledges the exact expire command, so an acceptance committed near the
 * deadline is never raced by a new offer.
 */
@Slf4j
final class OfferExpiry {

    /** Compensation entry point of the coordinator, used when the offer cannot be rematched. */
    @FunctionalInterface
    interface StepFailure {
        void fail(String stepName, SagaInstance saga, String reason, String rawEvent);
    }

    private final ObjectMapper objectMapper;
    private final DispatchCommandOutbox outbox;
    private final MatchingCommands matching;
    private final Consumer<SagaInstance> store;
    private final StepFailure stepFailure;
    private final java.util.function.IntSupplier maxFailedOffers;

    OfferExpiry(ObjectMapper objectMapper, DispatchCommandOutbox outbox, MatchingCommands matching,
                Consumer<SagaInstance> store, StepFailure stepFailure,
                java.util.function.IntSupplier maxFailedOffers) {
        this.objectMapper = objectMapper;
        this.outbox = outbox;
        this.matching = matching;
        this.store = store;
        this.stepFailure = stepFailure;
        this.maxFailedOffers = maxFailedOffers;
    }

    /** A timeout observation applies only to the exact status, version and update it was taken from. */
    static boolean matchesObservation(SagaInstance saga, SagaTimeoutCommand command) {
        if (saga.getStatus() != command.expectedStatus()
                || versionOf(saga) != command.expectedVersion()
                || saga.getUpdatedAt() == null) {
            return false;
        }
        return saga.getUpdatedAt().equals(command.observedAt());
    }

    static long versionOf(SagaInstance saga) {
        return saga.getVersion() == null ? 0L : saga.getVersion();
    }

    /**
     * A malformed persisted offer is treated as due so the timeout command can
     * fail closed through the normal compensation path instead of staying
     * invisible in the scheduler forever.
     */
    boolean isDue(SagaInstance saga) {
        CaseHistory.Fact foundStep = new JsonCaseHistory(saga, objectMapper).latestFact("SHIPPER_FOUND");
        if (foundStep == null || foundStep.eventData() == null) {
            return true;
        }
        try {
            JsonNode foundPayload = objectMapper.readTree(foundStep.eventData());
            Integer waitingTimeoutSeconds = foundPayload.hasNonNull("waitingTimeoutSeconds")
                    ? foundPayload.get("waitingTimeoutSeconds").asInt()
                    : null;
            LocalDateTime offerFoundAt = foundPayload.hasNonNull("foundAt")
                    ? LocalDateTime.parse(foundPayload.get("foundAt").asText())
                    : foundStep.executedAt();
            return ShipperOffer.isDue(offerFoundAt, waitingTimeoutSeconds, LocalDateTime.now());
        } catch (Exception malformed) {
            return true;
        }
    }

    /**
     * A shipper did not answer the single active offer. Re-run matching with the
     * timed-out shipper excluded; only compensate after the shared attempt limit.
     * The aggregate is already locked and the timeout has already been claimed.
     */
    void expire(SagaInstance saga, String rawTimeoutEvent) {
        Long orderId = saga.getOrderId();

        CaseHistory history = new JsonCaseHistory(saga, objectMapper);
        CaseHistory.Fact foundStep = history.latestFact("SHIPPER_FOUND");
        if (foundStep == null || foundStep.eventData() == null) {
            stepFailure.fail("SHIPPER_OFFER_TIMEOUT", saga,
                    "Missing shipper offer payload", rawTimeoutEvent);
            return;
        }

        long previousFailedOffers = history.countWithPrefix("SHIPPER_REJECTED")
                + history.countWithPrefix("SHIPPER_OFFER_TIMEOUT");
        // The shared limit is checked before parsing so an exhausted case with a
        // malformed offer still compensates as exhausted.
        if (previousFailedOffers >= maxFailedOffers.getAsInt()) {
            stepFailure.fail("SHIPPER_OFFER_TIMEOUT_LIMIT", saga,
                    "Shipper offer attempts exhausted", rawTimeoutEvent);
            return;
        }

        try {
            ObjectNode payload = matching.buildFindShipperPayload(saga, foundStep.eventData());
            JsonNode foundPayload = objectMapper.readTree(foundStep.eventData());
            if (!foundPayload.has("availableShippers")
                    || !foundPayload.get("availableShippers").isArray()
                    || foundPayload.get("availableShippers").size() != 1
                    || !foundPayload.get("availableShippers").get(0).hasNonNull("shipperId")) {
                throw new IllegalStateException("Offer payload must contain exactly one shipper");
            }
            long timedOutShipperId = foundPayload.get("availableShippers").get(0)
                    .get("shipperId").asLong();
            if (timedOutShipperId <= 0) {
                throw new IllegalStateException("Offer payload shipperId must be positive");
            }
            ShipperOffer offer = new ShipperOffer(timedOutShipperId,
                    foundPayload.hasNonNull("foundAt")
                            ? LocalDateTime.parse(foundPayload.get("foundAt").asText())
                            : foundStep.executedAt(),
                    foundPayload.hasNonNull("waitingTimeoutSeconds")
                            ? foundPayload.get("waitingTimeoutSeconds").asInt()
                            : null);
            LocalDateTime offerExpiresAt = offer.expiresAt();
            if (offerExpiresAt.isAfter(LocalDateTime.now())) {
                log.debug("[Saga] Offer is still active for orderId={} until {}, skipping timeout poll",
                        orderId, offerExpiresAt);
                return;
            }

            RematchPolicy.Rematch rematch = (RematchPolicy.Rematch) RematchPolicy.onOfferTimeout(
                    timedOutShipperId, history.recordedRejectedShippers(), previousFailedOffers,
                    maxFailedOffers.getAsInt());
            payload.put("rejectedShipperId", timedOutShipperId);

            var excludedArray = objectMapper.createArrayNode();
            rematch.excludedShipperIds().forEach(excludedArray::add);
            payload.set("excludedShipperIds", excludedArray);
            MatchingCommands.putRetrySettings(payload, MatchingRetrySettings.REMATCH);

            // The prepared rematch is persisted now but dispatched only after
            // Delivery acknowledges retirement (delivery.offer-retired), so an
            // acceptance committed near the deadline cannot race a new offer.
            String rematchEvent = objectMapper.writeValueAsString(payload);
            saga.setStatus(SagaStatus.OFFER_RETIRING);
            saga.addStep("SHIPPER_OFFER_TIMEOUT_" + rematch.attempt(),
                    "shipper.offer-timeout", rematchEvent);

            ObjectNode expireCommand = objectMapper.createObjectNode();
            expireCommand.put("orderId", orderId);
            expireCommand.put("deliveryId", saga.getDeliveryId());
            expireCommand.put("timedOutShipperId", timedOutShipperId);
            expireCommand.put("expectedOfferExpiresAt", offerExpiresAt.toString());
            UUID matchingSessionId = matching.currentSession(saga);
            if (matchingSessionId != null) {
                expireCommand.put("matchingSessionId", matchingSessionId.toString());
            }

            UUID expireCommandId = outbox.send(DispatchCoordinator.CMD_EXPIRE_SHIPPER_OFFER, orderId.toString(),
                    objectMapper.writeValueAsString(expireCommand));
            ObjectNode requested = objectMapper.createObjectNode();
            requested.put("expireCommandEventId", expireCommandId == null ? null : expireCommandId.toString());
            saga.addStep("OFFER_RETIRE_REQUESTED", DispatchCoordinator.CMD_EXPIRE_SHIPPER_OFFER, requested.toString());
            store.accept(saga);
            log.info("🔄 [Saga] Offer timed out for shipper {}, awaiting retirement before rematching "
                    + "orderId={} exclusions={}", timedOutShipperId, orderId, rematch.excludedShipperIds());
        } catch (Exception e) {
            DispatchCommandPublishException.rethrowIfPublishFailure(e);
            log.error("[Saga] Cannot build offer-timeout rematch command for orderId={}", orderId, e);
            stepFailure.fail("SHIPPER_OFFER_TIMEOUT", saga,
                    "Cannot build rematch command: " + e.getMessage(), rawTimeoutEvent);
        }
    }
}
