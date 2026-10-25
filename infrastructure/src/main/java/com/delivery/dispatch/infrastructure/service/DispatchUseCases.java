package com.delivery.dispatch.infrastructure.service;

import com.delivery.dispatch.application.DefaultAssignmentUseCase;
import com.delivery.dispatch.application.DefaultDeliveryCreationUseCase;
import com.delivery.dispatch.application.DefaultDeliveryProgressUseCase;
import com.delivery.dispatch.application.DefaultMatchOutcomeUseCase;
import com.delivery.dispatch.application.DefaultOrderLifecycleUseCase;
import com.delivery.dispatch.application.DefaultStepFailureUseCase;
import com.delivery.dispatch.application.api.AssignmentUseCase;
import com.delivery.dispatch.application.api.DeliveryCreationUseCase;
import com.delivery.dispatch.application.api.DeliveryProgressUseCase;
import com.delivery.dispatch.application.api.DispatchCase;
import com.delivery.dispatch.application.api.MatchOutcomeUseCase;
import com.delivery.dispatch.application.api.OfferCommands;
import com.delivery.dispatch.application.api.OrderLifecycleUseCase;
import com.delivery.dispatch.application.api.StepFailureUseCase;
import com.delivery.dispatch.domain.FailureCompensation;
import com.delivery.dispatch.domain.MatchingCommandPolicy;
import com.delivery.dispatch.domain.MatchingRetrySettings;
import com.delivery.dispatch.infrastructure.entity.SagaInstance;
import com.delivery.dispatch.infrastructure.entity.SagaInstance.SagaStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

/**
 * Binds the pure dispatch use cases (application module) to this adapter's
 * persistence and outbox commands. Each factory returns a use case whose side
 * effects are: save the case, write a command to the outbox, or send an
 * order-status update.
 */
@Slf4j
final class DispatchUseCases {

    private final Consumer<SagaInstance> store;
    private final Function<Long, SagaInstance> lockCase;
    private final ObjectMapper objectMapper;
    private final DispatchCommandOutbox outbox;
    private final MatchingCommands matching;
    private final BiPredicate<String, String> sameJson;
    private final Supplier<MatchingRetrySettings> initialRetrySettings;
    private final IntSupplier findingShipperTimeoutMinutes;
    private final IntSupplier maxFailedOffers;

    DispatchUseCases(Consumer<SagaInstance> store, Function<Long, SagaInstance> lockCase,
                     ObjectMapper objectMapper, DispatchCommandOutbox outbox, MatchingCommands matching,
                     BiPredicate<String, String> sameJson, Supplier<MatchingRetrySettings> initialRetrySettings,
                     IntSupplier findingShipperTimeoutMinutes, IntSupplier maxFailedOffers) {
        this.store = store;
        this.lockCase = lockCase;
        this.objectMapper = objectMapper;
        this.outbox = outbox;
        this.matching = matching;
        this.sameJson = sameJson;
        this.initialRetrySettings = initialRetrySettings;
        this.findingShipperTimeoutMinutes = findingShipperTimeoutMinutes;
        this.maxFailedOffers = maxFailedOffers;
    }

    DeliveryCreationUseCase deliveryCreation() {
        return new DefaultDeliveryCreationUseCase(
                dispatchCase -> store.accept(saga(dispatchCase)),
                (dispatchCase, deliveryEvent) -> {
                    SagaInstance saga = saga(dispatchCase);
                    startMatching(saga, saga.getOrderId(), saga.getDeliveryId(), deliveryEvent);
                },
                (dispatchCase, cause) -> outbox.send(DispatchCoordinator.CMD_CANCEL_DELIVERY,
                        String.valueOf(dispatchCase.orderId()), cause),
                (dispatchCase, status, cause) -> outbox.orderStatus(saga(dispatchCase), status, cause));
    }

    OrderLifecycleUseCase orderLifecycle() {
        return new DefaultOrderLifecycleUseCase(
                dispatchCase -> store.accept(saga(dispatchCase)),
                (dispatchCase, deliveryEvent) -> {
                    SagaInstance saga = saga(dispatchCase);
                    startMatching(saga, saga.getOrderId(), saga.getDeliveryId(), deliveryEvent);
                },
                (dispatchCase, cause) -> cancelDeliveryAndStopMatching(saga(dispatchCase), cause),
                sameJson::test);
    }

    MatchOutcomeUseCase matchOutcomes() {
        return new DefaultMatchOutcomeUseCase(
                dispatchCase -> store.accept(saga(dispatchCase)),
                offerCommands(),
                (dispatchCase, status, cause) -> outbox.orderStatus(saga(dispatchCase), status, cause));
    }

    AssignmentUseCase assignments() {
        return new DefaultAssignmentUseCase(
                dispatchCase -> store.accept(saga(dispatchCase)),
                (dispatchCase, cause, excluded) -> rematchAfterRejection(saga(dispatchCase), cause, excluded),
                offerCommands(),
                (dispatchCase, status, cause) -> outbox.orderStatus(saga(dispatchCase), status, cause),
                maxFailedOffers.getAsInt());
    }

    DeliveryProgressUseCase deliveryProgress() {
        return new DefaultDeliveryProgressUseCase(
                dispatchCase -> store.accept(saga(dispatchCase)),
                (dispatchCase, status, cause) -> outbox.orderStatus(saga(dispatchCase), status, cause),
                sameJson::test);
    }

    StepFailureUseCase stepFailures() {
        return new DefaultStepFailureUseCase(
                dispatchCase -> store.accept(saga(dispatchCase)),
                (dispatchCase, deliveryCommand, stopMatching, cause) ->
                        compensateDelivery(saga(dispatchCase), deliveryCommand, stopMatching, cause),
                (dispatchCase, status, cause) -> outbox.orderStatus(saga(dispatchCase), status, cause));
    }

    /**
     * Phát lệnh tìm shipper (kèm cấu hình retry) + chuyển saga sang FINDING_SHIPPER.
     * Dùng chung cho: delivery-created (đã confirm) và restaurant-confirmed (delivery đã tạo).
     */
    private void startMatching(SagaInstance saga, Long orderId, Long deliveryId, String deliveryResultEvent) {
        String modifiedEvent;
        try {
            ObjectNode payloadNode = matching.buildFindShipperPayload(saga, deliveryResultEvent);
            MatchingCommands.putRetrySettings(payloadNode, initialRetrySettings.get());
            payloadNode.put("matchingDeadlineAt", MatchingCommandPolicy
                    .initialDeadline(LocalDateTime.now(), findingShipperTimeoutMinutes.getAsInt()).toString());

            modifiedEvent = matching.sendFindShipper(saga, orderId, payloadNode);
            outbox.orderStatus(lockCase.apply(orderId), "FINDING_SHIPPER", modifiedEvent);
            log.info("📤 [Saga] Sent command: {} for orderId={}, deliveryId={} with retry settings",
                    DispatchCoordinator.CMD_FIND_SHIPPER, orderId, deliveryId);
        } catch (Exception e) {
            DispatchCommandPublishException.rethrowIfPublishFailure(e);
            throw new IllegalStateException("Cannot build canonical find-shipper command", e);
        }

        saga.setStatus(SagaStatus.FINDING_SHIPPER);
        store.accept(saga);
    }

    /** Canonical rematch with the fixed rematch budget and ordered exclusions. */
    private void rematchAfterRejection(SagaInstance saga, String causeEvent, List<Long> excludedShipperIds) {
        Long orderId = saga.getOrderId();
        try {
            ObjectNode payloadNode = matching.buildFindShipperPayload(saga, causeEvent);
            MatchingCommands.putRetrySettings(payloadNode, MatchingRetrySettings.REMATCH);
            var excludedArray = objectMapper.createArrayNode();
            excludedShipperIds.forEach(excludedArray::add);
            payloadNode.set("excludedShipperIds", excludedArray);
            matching.sendFindShipper(saga, orderId, payloadNode);
        } catch (Exception e) {
            DispatchCommandPublishException.rethrowIfPublishFailure(e);
            throw new IllegalStateException("Cannot build canonical rematch command for orderId=" + orderId, e);
        }
    }

    private OfferCommands offerCommands() {
        return new OfferCommands() {
            @Override
            public void requestOfferPersistence(DispatchCase dispatchCase, String shipperFoundEvent) {
                SagaInstance saga = saga(dispatchCase);
                UUID cacheCommandId = outbox.send(DispatchCoordinator.CMD_CACHE_SHIPPER_FOUND,
                        saga.getOrderId().toString(), shipperFoundEvent);
                ObjectNode requested = objectMapper.createObjectNode();
                requested.put("cacheCommandEventId", cacheCommandId.toString());
                try {
                    requested.put("matchingSessionId",
                            objectMapper.readTree(shipperFoundEvent).path("matchingSessionId").asText());
                } catch (Exception invalid) {
                    throw new IllegalArgumentException("Invalid shipper.found payload", invalid);
                }
                saga.addStep("OFFER_PERSIST_REQUESTED", DispatchCoordinator.CMD_CACHE_SHIPPER_FOUND, requested.toString());
            }

            @Override
            public void markShipperNotFound(DispatchCase dispatchCase, String causeEvent) {
                outbox.send(DispatchCoordinator.CMD_MARK_SHIPPER_NOT_FOUND, String.valueOf(dispatchCase.orderId()),
                        causeEvent);
            }

            @Override
            public String startPreparedRematch(DispatchCase dispatchCase, String preparedCommand) {
                SagaInstance saga = saga(dispatchCase);
                try {
                    return matching.sendFindShipper(saga, saga.getOrderId(),
                            (ObjectNode) objectMapper.readTree(preparedCommand));
                } catch (DispatchCommandPublishException publishException) {
                    throw publishException;
                } catch (Exception malformed) {
                    throw new IllegalStateException("Prepared rematch is malformed for orderId="
                            + saga.getOrderId(), malformed);
                }
            }
        };
    }

    /** Cancels the Delivery and stops the current matching generation, enriched with deliveryId. */
    private void cancelDeliveryAndStopMatching(SagaInstance saga, String rawEvent) {
        Long orderId = saga.getOrderId();
        try {
            ObjectNode payloadNode = (ObjectNode) objectMapper.readTree(rawEvent);
            if (saga.getDeliveryId() != null) {
                payloadNode.put("deliveryId", saga.getDeliveryId());
            }
            String enrichedEvent = objectMapper.writeValueAsString(payloadNode);
            outbox.send(DispatchCoordinator.CMD_CANCEL_DELIVERY, orderId.toString(), enrichedEvent);
            matching.sendStopMatching(saga, orderId, enrichedEvent);
        } catch (Exception e) {
            DispatchCommandPublishException.rethrowIfPublishFailure(e);
            outbox.send(DispatchCoordinator.CMD_CANCEL_DELIVERY, orderId.toString(), rawEvent);
            matching.sendStopMatching(saga, orderId, rawEvent);
        }
    }

    /** Sends the compensating Delivery command enriched with deliveryId; returns the correlated event. */
    private String compensateDelivery(SagaInstance saga, FailureCompensation.DeliveryCommand deliveryCommand,
                                      boolean stopMatching, String rawEvent) {
        Long orderId = saga.getOrderId();
        String topic = deliveryCommand == FailureCompensation.DeliveryCommand.CANCEL_DELIVERY
                ? DispatchCoordinator.CMD_CANCEL_DELIVERY
                : DispatchCoordinator.CMD_MARK_SHIPPER_NOT_FOUND;
        String correlatedEvent = rawEvent;
        try {
            ObjectNode payloadNode = (ObjectNode) objectMapper.readTree(rawEvent);
            if (saga.getDeliveryId() != null) {
                payloadNode.put("deliveryId", saga.getDeliveryId());
            }
            String enrichedEvent = objectMapper.writeValueAsString(payloadNode);
            correlatedEvent = enrichedEvent;
            outbox.send(topic, orderId.toString(), enrichedEvent);
            if (stopMatching) {
                matching.sendStopMatching(saga, orderId, enrichedEvent);
            }
        } catch (Exception e) {
            DispatchCommandPublishException.rethrowIfPublishFailure(e);
            outbox.send(topic, orderId.toString(), rawEvent);
        }
        return correlatedEvent;
    }

    private static SagaInstance saga(DispatchCase dispatchCase) {
        return ((SagaDispatchCase) dispatchCase).saga();
    }
}
