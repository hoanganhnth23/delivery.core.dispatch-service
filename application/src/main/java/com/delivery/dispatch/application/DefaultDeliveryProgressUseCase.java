package com.delivery.dispatch.application;

import com.delivery.dispatch.application.api.DeliveryProgressUseCase;
import com.delivery.dispatch.application.api.DispatchCase;
import com.delivery.dispatch.application.api.DispatchCaseStore;
import com.delivery.dispatch.application.api.DispatchCommands;
import com.delivery.dispatch.application.api.EventEquality;
import com.delivery.dispatch.domain.DeliveryProgressPolicy;
import com.delivery.dispatch.domain.DispatchStatus;

import java.util.Objects;

/**
 * Delivery status progression: exact replays are skipped, contradictory
 * replays fail, a cancellation completes compensation, the SHIPPER_NOT_FOUND
 * echo is recorded without a second Order command, and every other status
 * advances the case strictly and is forwarded to Order.
 */
public final class DefaultDeliveryProgressUseCase implements DeliveryProgressUseCase {

    static final String EVENT_TYPE = "delivery.status-updated";

    private final DispatchCaseStore store;
    private final DispatchCommands commands;
    private final EventEquality equality;

    public DefaultDeliveryProgressUseCase(DispatchCaseStore store, DispatchCommands commands, EventEquality equality) {
        this.store = Objects.requireNonNull(store, "store");
        this.commands = Objects.requireNonNull(commands, "commands");
        this.equality = Objects.requireNonNull(equality, "equality");
    }

    @Override
    public Outcome apply(DispatchCase dispatchCase, String deliveryStatus, String rawEvent) {
        long orderId = dispatchCase.orderId();
        if (DeliveryProgressPolicy.isShipperNotFoundEcho(deliveryStatus)) {
            return recordShipperNotFoundEcho(dispatchCase, rawEvent);
        }
        DispatchStatus target = DeliveryProgressPolicy.targetFor(deliveryStatus);
        String stepName = "DELIVERY_" + deliveryStatus;
        String applied = dispatchCase.history().latest(stepName);
        if (applied != null) {
            if (equality.same(applied, rawEvent)) {
                return Outcome.REPLAY;
            }
            throw new IllegalStateException("Conflicting delivery status replay " + deliveryStatus
                    + " for orderId=" + orderId);
        }
        if (target == DispatchStatus.CANCELLED && DeliveryProgressPolicy.isCancellationConfirmation(
                dispatchCase.status(), dispatchCase.history().has("ORDER_CANCELLED"))) {
            if (dispatchCase.status() == DispatchStatus.COMPENSATING) {
                dispatchCase.transitionTo(DispatchStatus.CANCELLED);
                dispatchCase.markCompleted();
            }
            dispatchCase.record(stepName, EVENT_TYPE, rawEvent);
            store.save(dispatchCase);
            return Outcome.CANCELLATION_CONFIRMED;
        }
        if (dispatchCase.status().isTerminal()) {
            throw new IllegalStateException("Terminal saga " + dispatchCase.status()
                    + " cannot apply delivery status " + deliveryStatus + " for orderId=" + orderId);
        }
        DeliveryProgressPolicy.requireTransition(dispatchCase.status(), target, orderId);
        dispatchCase.transitionTo(target);
        if (target == DispatchStatus.COMPLETED || target == DispatchStatus.CANCELLED) {
            dispatchCase.markCompleted();
        }
        dispatchCase.record(stepName, EVENT_TYPE, rawEvent);
        store.save(dispatchCase);
        commands.orderStatus(dispatchCase, deliveryStatus, rawEvent);
        return Outcome.APPLIED;
    }

    private Outcome recordShipperNotFoundEcho(DispatchCase dispatchCase, String rawEvent) {
        long orderId = dispatchCase.orderId();
        String stepName = "DELIVERY_SHIPPER_NOT_FOUND";
        String applied = dispatchCase.history().latest(stepName);
        if (applied != null) {
            if (equality.same(applied, rawEvent)) {
                return Outcome.REPLAY;
            }
            throw new IllegalStateException("Conflicting delivery SHIPPER_NOT_FOUND replay for orderId=" + orderId);
        }
        if (dispatchCase.status() != DispatchStatus.FAILED || !dispatchCase.history().has("SHIPPER_NOT_FOUND")) {
            throw new IllegalStateException("Delivery SHIPPER_NOT_FOUND status must follow shipper.not-found "
                    + "for orderId=" + orderId);
        }
        // Order already converged on shipper.not-found; this echo only informs Notification.
        dispatchCase.record(stepName, EVENT_TYPE, rawEvent);
        store.save(dispatchCase);
        return Outcome.SHIPPER_NOT_FOUND_ECHO_RECORDED;
    }
}
