package com.delivery.dispatch.application;

import com.delivery.dispatch.application.api.DispatchCase;
import com.delivery.dispatch.application.api.DispatchCaseStore;
import com.delivery.dispatch.application.api.DispatchCommands;
import com.delivery.dispatch.application.api.FailureCommands;
import com.delivery.dispatch.application.api.StepFailureUseCase;
import com.delivery.dispatch.domain.DispatchStatus;
import com.delivery.dispatch.domain.FailureCompensation;

import java.util.Objects;

/**
 * Compensation is chosen from the status observed before the failure: a
 * Delivery that never entered matching is cancelled; once matching started
 * the case converges to SHIPPER_NOT_FOUND and the generation is stopped.
 * A Delivery refusal to cancel after compensation is recorded for manual
 * reconciliation instead of being ignored.
 */
public final class DefaultStepFailureUseCase implements StepFailureUseCase {

    private final DispatchCaseStore store;
    private final FailureCommands failures;
    private final DispatchCommands commands;

    public DefaultStepFailureUseCase(DispatchCaseStore store, FailureCommands failures, DispatchCommands commands) {
        this.store = Objects.requireNonNull(store, "store");
        this.failures = Objects.requireNonNull(failures, "failures");
        this.commands = Objects.requireNonNull(commands, "commands");
    }

    @Override
    public Outcome onStepFailed(DispatchCase dispatchCase, String stepName, String rawEvent) {
        switch (FailureCompensation.outcome(stepName, dispatchCase.status())) {
            case RECORD_CANCEL_REFUSAL -> {
                dispatchCase.record("DELIVERY_CANCEL_FAILED", "delivery.cancel.failed", rawEvent);
                dispatchCase.transitionTo(DispatchStatus.FAILED);
                dispatchCase.markCompleted();
                store.save(dispatchCase);
                return Outcome.CANCEL_REFUSAL_RECORDED;
            }
            case IGNORE_TERMINAL -> {
                return Outcome.IGNORED_TERMINAL;
            }
            default -> {
                DispatchStatus previous = dispatchCase.status();
                dispatchCase.transitionTo(DispatchStatus.COMPENSATING);
                dispatchCase.record(stepName + "_FAILED", stepName + ".failed", rawEvent);
                FailureCompensation compensation = FailureCompensation.forPreviousStatus(previous);
                String orderCause = rawEvent;
                if (compensation.deliveryCommand() != FailureCompensation.DeliveryCommand.NONE) {
                    orderCause = failures.compensate(dispatchCase, compensation.deliveryCommand(),
                            compensation.stopMatching(), rawEvent);
                }
                commands.orderStatus(dispatchCase, compensation.orderStatus(), orderCause);
                dispatchCase.transitionTo(DispatchStatus.FAILED);
                dispatchCase.markCompleted();
                store.save(dispatchCase);
                return Outcome.COMPENSATED;
            }
        }
    }
}
