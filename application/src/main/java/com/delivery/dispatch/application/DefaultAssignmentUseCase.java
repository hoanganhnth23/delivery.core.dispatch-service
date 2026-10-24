package com.delivery.dispatch.application;

import com.delivery.dispatch.application.api.AssignmentUseCase;
import com.delivery.dispatch.application.api.DispatchCase;
import com.delivery.dispatch.application.api.DispatchCaseStore;
import com.delivery.dispatch.application.api.DispatchCommands;
import com.delivery.dispatch.application.api.OfferCommands;
import com.delivery.dispatch.application.api.RematchCommands;
import com.delivery.dispatch.domain.AssignmentPolicy;
import com.delivery.dispatch.domain.CaseHistory;
import com.delivery.dispatch.domain.DispatchStatus;
import com.delivery.dispatch.domain.RematchPolicy;

import java.util.Objects;

/**
 * Acceptance assigns the shipper unless it is a replay, a stale acceptance
 * from a rejecting shipper, or the case is no longer awaiting a decision.
 * Rejection (or cancel-assignment) rematches with exclusions until the
 * shared attempt limit, then fails as SHIPPER_NOT_FOUND.
 */
public final class DefaultAssignmentUseCase implements AssignmentUseCase {

    private final DispatchCaseStore store;
    private final RematchCommands rematch;
    private final OfferCommands offers;
    private final DispatchCommands commands;

    public DefaultAssignmentUseCase(DispatchCaseStore store, RematchCommands rematch, OfferCommands offers,
                                    DispatchCommands commands) {
        this.store = Objects.requireNonNull(store, "store");
        this.rematch = Objects.requireNonNull(rematch, "rematch");
        this.offers = Objects.requireNonNull(offers, "offers");
        this.commands = Objects.requireNonNull(commands, "commands");
    }

    @Override
    public AcceptanceOutcome onAccepted(DispatchCase dispatchCase, Long shipperId, String rawEvent) {
        if (shipperId == null || shipperId <= 0) {
            throw new IllegalArgumentException("shipperId must be positive");
        }
        CaseHistory history = dispatchCase.history();
        Long assigned = assignedShipper(dispatchCase);
        switch (AssignmentPolicy.onAcceptance(dispatchCase.orderId(), dispatchCase.status(), shipperId, assigned,
                history.has("SHIPPER_ASSIGNED"), history.rejectingShippers().contains(shipperId))) {
            case REPLAY -> {
                return AcceptanceOutcome.REPLAY;
            }
            case IGNORE_REJECTED_SHIPPER -> {
                return AcceptanceOutcome.IGNORED_REJECTED_SHIPPER;
            }
            case IGNORE_STATE -> {
                return AcceptanceOutcome.IGNORED_STATE;
            }
            default -> {
                dispatchCase.transitionTo(DispatchStatus.SHIPPER_ASSIGNED);
                dispatchCase.assignShipper(shipperId);
                dispatchCase.record("SHIPPER_ASSIGNED", "delivery.shipper-accepted", rawEvent);
                store.save(dispatchCase);
                commands.orderStatus(dispatchCase, "SHIPPER_ASSIGNED", rawEvent);
                return AcceptanceOutcome.ASSIGNED;
            }
        }
    }

    @Override
    public RejectionOutcome onRejected(DispatchCase dispatchCase, Long rejectedShipperId, String rawEvent) {
        if (!AssignmentPolicy.acceptsRejection(dispatchCase.orderId(), dispatchCase.status(),
                assignedShipper(dispatchCase), rejectedShipperId)) {
            return RejectionOutcome.IGNORED_STATE;
        }
        CaseHistory history = dispatchCase.history();
        long previousRejections = history.countWithPrefix("SHIPPER_REJECTED");
        RematchPolicy.Decision decision = RematchPolicy.onRejection(
                rejectedShipperId, history.rejectingShippers(), previousRejections);
        if (decision instanceof RematchPolicy.Duplicate) {
            return RejectionOutcome.DUPLICATE;
        }
        if (decision instanceof RematchPolicy.Exhausted) {
            dispatchCase.transitionTo(DispatchStatus.FAILED);
            dispatchCase.markCompleted();
            dispatchCase.record("SHIPPER_REJECTED_LIMIT", "delivery.shipper-rejected", rawEvent);
            store.save(dispatchCase);
            offers.markShipperNotFound(dispatchCase, rawEvent);
            commands.orderStatus(dispatchCase, "SHIPPER_NOT_FOUND", rawEvent);
            return RejectionOutcome.EXHAUSTED;
        }
        RematchPolicy.Rematch next = (RematchPolicy.Rematch) decision;
        dispatchCase.transitionTo(DispatchStatus.FINDING_SHIPPER);
        dispatchCase.assignShipper(null);
        dispatchCase.record("SHIPPER_REJECTED_" + next.attempt(), "delivery.shipper-rejected", rawEvent);
        store.save(dispatchCase);
        rematch.rematchAfterRejection(dispatchCase, rawEvent, next.excludedShipperIds());
        store.save(dispatchCase);
        commands.orderStatus(dispatchCase, "FINDING_SHIPPER", rawEvent);
        return RejectionOutcome.REMATCHED;
    }

    private static Long assignedShipper(DispatchCase dispatchCase) {
        return dispatchCase.assignedShipper();
    }
}
