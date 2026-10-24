package com.delivery.dispatch.application.api;

/** Starts shipper matching for a case whose Delivery exists and restaurant confirmed. */
@FunctionalInterface
public interface MatchingCommands {

    void startMatching(DispatchCase dispatchCase, String deliveryResultEvent);
}
