package com.delivery.dispatch.application.api;

/** Cancels a Delivery created after its case already ended. */
@FunctionalInterface
public interface DeliveryCancellationCommands {

    void cancelOrphanDelivery(DispatchCase dispatchCase, String causeEvent);
}
