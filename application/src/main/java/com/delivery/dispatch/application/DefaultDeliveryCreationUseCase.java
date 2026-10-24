package com.delivery.dispatch.application;

import com.delivery.dispatch.application.api.DeliveryCancellationCommands;
import com.delivery.dispatch.application.api.DeliveryCreationUseCase;
import com.delivery.dispatch.application.api.DispatchCase;
import com.delivery.dispatch.application.api.DispatchCaseStore;
import com.delivery.dispatch.application.api.DispatchCommands;
import com.delivery.dispatch.application.api.MatchingCommands;
import com.delivery.dispatch.domain.DispatchStatus;

import java.util.Objects;

/**
 * A created Delivery waits for the restaurant gate (or matches at once when
 * the restaurant already confirmed). A result that arrives after the case
 * was cancelled or failed is recorded and its Delivery cancelled so it
 * cannot be orphaned; any other second identity is contradictory.
 */
public final class DefaultDeliveryCreationUseCase implements DeliveryCreationUseCase {

    static final String DELIVERY_CREATED = "DELIVERY_CREATED";
    static final String EVENT_TYPE = "delivery.created.result";

    private final DispatchCaseStore store;
    private final MatchingCommands matching;
    private final DeliveryCancellationCommands cancellations;
    private final DispatchCommands commands;

    public DefaultDeliveryCreationUseCase(DispatchCaseStore store, MatchingCommands matching,
                                          DeliveryCancellationCommands cancellations, DispatchCommands commands) {
        this.store = Objects.requireNonNull(store, "store");
        this.matching = Objects.requireNonNull(matching, "matching");
        this.cancellations = Objects.requireNonNull(cancellations, "cancellations");
        this.commands = Objects.requireNonNull(commands, "commands");
    }

    @Override
    public CreatedOutcome onDeliveryCreated(DispatchCase dispatchCase, Long deliveryId, String rawEvent) {
        long orderId = dispatchCase.orderId();
        DispatchStatus status = dispatchCase.status();
        boolean recorded = dispatchCase.history().has(DELIVERY_CREATED);
        if (status != DispatchStatus.STARTED) {
            if (deliveryId != null && deliveryId.equals(dispatchCase.deliveryId()) && recorded) {
                return CreatedOutcome.REPLAY;
            }
            if ((status == DispatchStatus.CANCELLED || status == DispatchStatus.FAILED)
                    && dispatchCase.deliveryId() == null && !recorded) {
                if (deliveryId == null || deliveryId <= 0) {
                    throw new IllegalArgumentException("deliveryId must be positive for cancelled orderId=" + orderId);
                }
                dispatchCase.attachDelivery(deliveryId);
                dispatchCase.record(DELIVERY_CREATED, EVENT_TYPE, rawEvent);
                store.save(dispatchCase);
                cancellations.cancelOrphanDelivery(dispatchCase, rawEvent);
                return CreatedOutcome.ORPHAN_CANCELLED;
            }
            throw new IllegalStateException("Contradictory delivery-created event for order "
                    + orderId + ": existing delivery=" + dispatchCase.deliveryId()
                    + ", received delivery=" + deliveryId + ", saga status=" + status);
        }
        dispatchCase.attachDelivery(deliveryId);
        dispatchCase.transitionTo(DispatchStatus.DELIVERY_CREATED);
        dispatchCase.record(DELIVERY_CREATED, EVENT_TYPE, rawEvent);
        store.save(dispatchCase);
        if (!dispatchCase.history().has("RESTAURANT_CONFIRMED")) {
            return CreatedOutcome.AWAITING_RESTAURANT;
        }
        matching.startMatching(dispatchCase, rawEvent);
        return CreatedOutcome.MATCHING_STARTED;
    }

    @Override
    public FailedOutcome onDeliveryCreationFailed(DispatchCase dispatchCase, String rawEvent) {
        if (dispatchCase.status() != DispatchStatus.STARTED) {
            return FailedOutcome.IGNORED_STATE;
        }
        dispatchCase.transitionTo(DispatchStatus.COMPENSATING);
        dispatchCase.record("DELIVERY_CREATION_FAILED", "delivery.created.failed", rawEvent);
        store.save(dispatchCase);
        commands.orderStatus(dispatchCase, "CANCELLED", rawEvent);
        dispatchCase.transitionTo(DispatchStatus.FAILED);
        dispatchCase.markCompleted();
        store.save(dispatchCase);
        return FailedOutcome.COMPENSATED;
    }
}
