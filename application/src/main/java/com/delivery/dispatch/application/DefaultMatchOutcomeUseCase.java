package com.delivery.dispatch.application;

import com.delivery.dispatch.application.api.DispatchCase;
import com.delivery.dispatch.application.api.DispatchCaseStore;
import com.delivery.dispatch.application.api.DispatchCommands;
import com.delivery.dispatch.application.api.MatchOutcomeUseCase;
import com.delivery.dispatch.application.api.OfferCommands;
import com.delivery.dispatch.domain.DispatchStatus;
import com.delivery.dispatch.domain.OfferRetirementPolicy;

import java.util.Objects;
import java.util.UUID;
import java.util.function.BooleanSupplier;

/**
 * A found shipper becomes an offer only after Delivery persists it; Order
 * sees WAIT_SHIPPER_CONFIRM only on that confirmation. Not-found is a
 * terminal matching outcome. An expired offer is rematched only after
 * Delivery acknowledges the exact expire command.
 */
public final class DefaultMatchOutcomeUseCase implements MatchOutcomeUseCase {

    private final DispatchCaseStore store;
    private final OfferCommands offers;
    private final DispatchCommands commands;

    public DefaultMatchOutcomeUseCase(DispatchCaseStore store, OfferCommands offers, DispatchCommands commands) {
        this.store = Objects.requireNonNull(store, "store");
        this.offers = Objects.requireNonNull(offers, "offers");
        this.commands = Objects.requireNonNull(commands, "commands");
    }

    @Override
    public Outcome onShipperFound(DispatchCase dispatchCase, BooleanSupplier currentGeneration, String rawEvent) {
        if (!currentGeneration.getAsBoolean()) return Outcome.STALE;
        if (dispatchCase.status() != DispatchStatus.FINDING_SHIPPER) return Outcome.IGNORED_STATE;
        dispatchCase.transitionTo(DispatchStatus.OFFER_PERSISTING);
        dispatchCase.record("SHIPPER_FOUND", "shipper.found", rawEvent);
        offers.requestOfferPersistence(dispatchCase, rawEvent);
        store.save(dispatchCase);
        return Outcome.OFFER_PERSIST_REQUESTED;
    }

    @Override
    public Outcome onShipperNotFound(DispatchCase dispatchCase, BooleanSupplier currentGeneration, String rawEvent) {
        if (!currentGeneration.getAsBoolean()) return Outcome.STALE;
        if (dispatchCase.status() != DispatchStatus.FINDING_SHIPPER) return Outcome.IGNORED_STATE;
        dispatchCase.transitionTo(DispatchStatus.FAILED);
        dispatchCase.markCompleted();
        dispatchCase.record("SHIPPER_NOT_FOUND", "shipper.not-found", rawEvent);
        store.save(dispatchCase);
        // Distinct from cancellation so Delivery and Order both converge on SHIPPER_NOT_FOUND.
        offers.markShipperNotFound(dispatchCase, rawEvent);
        commands.orderStatus(dispatchCase, "SHIPPER_NOT_FOUND", rawEvent);
        return Outcome.SHIPPER_NOT_FOUND;
    }

    @Override
    public Outcome onOfferPersisted(DispatchCase dispatchCase, UUID sourceCommandId,
                                    BooleanSupplier currentGeneration, String rawEvent) {
        DispatchStatus status = dispatchCase.status();
        if (status == DispatchStatus.SHIPPER_ASSIGNED || status == DispatchStatus.CANCELLED
                || status == DispatchStatus.FAILED) {
            return Outcome.STRONGER_STATE;
        }
        if (status != DispatchStatus.OFFER_PERSISTING
                || !currentGeneration.getAsBoolean()
                || !sourceCommandId.toString().equals(
                        dispatchCase.history().latestField("OFFER_PERSIST_REQUESTED", "cacheCommandEventId"))) {
            return Outcome.STALE;
        }
        dispatchCase.transitionTo(DispatchStatus.SHIPPER_FOUND);
        dispatchCase.record("OFFER_PERSISTED", "delivery.offer-persisted", rawEvent);
        store.save(dispatchCase);
        commands.orderStatus(dispatchCase, "WAIT_SHIPPER_CONFIRM", rawEvent);
        return Outcome.OFFER_CONFIRMED;
    }

    @Override
    public Outcome onOfferRetired(DispatchCase dispatchCase, UUID sourceCommandId,
                                  OfferRetirementPolicy.Decision decision, Long shipperId, String rawEvent) {
        if (dispatchCase.status() != DispatchStatus.OFFER_RETIRING
                || !sourceCommandId.toString().equals(
                        dispatchCase.history().latestField("OFFER_RETIRE_REQUESTED", "expireCommandEventId"))) {
            return Outcome.STALE;
        }
        switch (decision) {
            case REMATCH -> {
                String prepared = dispatchCase.history().latestWithPrefix("SHIPPER_OFFER_TIMEOUT_");
                if (prepared == null) {
                    throw new IllegalStateException("Offer retirement has no prepared rematch for orderId="
                            + dispatchCase.orderId());
                }
                dispatchCase.transitionTo(DispatchStatus.FINDING_SHIPPER);
                dispatchCase.record("OFFER_RETIRED", "delivery.offer-retired", rawEvent);
                String findCommand = offers.startPreparedRematch(dispatchCase, prepared);
                store.save(dispatchCase);
                commands.orderStatus(dispatchCase, "FINDING_SHIPPER", findCommand);
                return Outcome.REMATCHED;
            }
            case ASSIGN -> {
                // Delivery committed the acceptance before the expiry; it is authoritative.
                dispatchCase.transitionTo(DispatchStatus.SHIPPER_ASSIGNED);
                dispatchCase.assignShipper(shipperId);
                dispatchCase.record("SHIPPER_ASSIGNED", "delivery.offer-retired", rawEvent);
                store.save(dispatchCase);
                commands.orderStatus(dispatchCase, "SHIPPER_ASSIGNED", rawEvent);
                return Outcome.ASSIGNED;
            }
            default -> {
                dispatchCase.record("OFFER_RETIRED_TERMINAL", "delivery.offer-retired", rawEvent);
                store.save(dispatchCase);
                return Outcome.TERMINAL_RECORDED;
            }
        }
    }
}
