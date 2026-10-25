package com.delivery.dispatch.application.api;

/** Delivery-owned shipper acceptance and rejection / cancel-assignment facts. */
public interface AssignmentUseCase {

    enum AcceptanceOutcome { REPLAY, IGNORED_REJECTED_SHIPPER, IGNORED_STATE, ASSIGNED }

    enum RejectionOutcome { IGNORED_STATE, DUPLICATE, EXHAUSTED, REMATCHED }

    AcceptanceOutcome onAccepted(DispatchCase dispatchCase, Long shipperId, String rawEvent);

    RejectionOutcome onRejected(DispatchCase dispatchCase, Long rejectedShipperId, String rawEvent);
}
