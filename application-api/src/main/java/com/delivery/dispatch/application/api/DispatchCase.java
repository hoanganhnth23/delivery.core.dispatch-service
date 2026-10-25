package com.delivery.dispatch.application.api;

import com.delivery.dispatch.domain.CaseHistory;
import com.delivery.dispatch.domain.DispatchStatus;

/** One locked coordination case as seen by use cases. */
public interface DispatchCase {

    long orderId();

    /** Delivery identity once Delivery confirmed creation; null before. */
    Long deliveryId();

    DispatchStatus status();

    void transitionTo(DispatchStatus status);

    /** Shipper currently holding the assignment, or null. */
    Long assignedShipper();

    /** Records the shipper holding the assignment (null clears it). */
    void assignShipper(Long shipperId);

    /** Stamps the completion time of a terminal case. */
    void markCompleted();

    /** Records the single Delivery identity of the case. */
    void attachDelivery(Long deliveryId);

    /** Clears completion while compensation is still awaited. */
    void clearCompletion();

    CaseHistory history();

    /** Appends one fact to the case history. */
    void record(String stepName, String eventType, String eventData);
}
