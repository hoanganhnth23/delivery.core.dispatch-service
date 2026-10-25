package com.delivery.dispatch.application.api;

import com.delivery.dispatch.domain.FailureCompensation;

/** Compensation commands chosen from the status before a step failed. */
@FunctionalInterface
public interface FailureCommands {

    /**
     * Issues the Delivery command (and the generation-scoped stop when requested).
     *
     * @return the cause event correlated with the case's delivery identity, for the Order command
     */
    String compensate(DispatchCase dispatchCase, FailureCompensation.DeliveryCommand deliveryCommand,
                      boolean stopMatching, String causeEvent);
}
