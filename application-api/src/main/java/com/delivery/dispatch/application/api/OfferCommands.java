package com.delivery.dispatch.application.api;

/** Commands around the single active offer and terminal matching outcomes. */
public interface OfferCommands {

    /** Asks Delivery to persist the found offer and records the awaited command identity. */
    void requestOfferPersistence(DispatchCase dispatchCase, String shipperFoundEvent);

    /** Moves Delivery to SHIPPER_NOT_FOUND (a matching outcome, not a cancellation). */
    void markShipperNotFound(DispatchCase dispatchCase, String causeEvent);

    /** Dispatches the rematch prepared at offer timeout; returns the emitted find command. */
    String startPreparedRematch(DispatchCase dispatchCase, String preparedCommand);
}
