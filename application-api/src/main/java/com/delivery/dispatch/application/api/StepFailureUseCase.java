package com.delivery.dispatch.application.api;

/** A failed step (timeout, Delivery refusal, unbuildable command) compensates the case. */
public interface StepFailureUseCase {

    enum Outcome { CANCEL_REFUSAL_RECORDED, IGNORED_TERMINAL, COMPENSATED }

    Outcome onStepFailed(DispatchCase dispatchCase, String stepName, String rawEvent);
}
