package com.delivery.dispatch.application.api;

import java.util.List;

/** Issues a new matching generation after a shipper rejected or cancelled the assignment. */
@FunctionalInterface
public interface RematchCommands {

    /** Rebuilds the canonical command with the rematch budget and exclusions, then dispatches it. */
    void rematchAfterRejection(DispatchCase dispatchCase, String causeEvent, List<Long> excludedShipperIds);
}
