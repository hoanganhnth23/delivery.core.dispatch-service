package com.delivery.dispatch.application.api;

/** Compensation commands issued when an order is cancelled. */
@FunctionalInterface
public interface CompensationCommands {

    /** Cancels the Delivery and stops exactly the current matching generation. */
    void cancelDeliveryAndStopMatching(DispatchCase dispatchCase, String causeEvent);
}
