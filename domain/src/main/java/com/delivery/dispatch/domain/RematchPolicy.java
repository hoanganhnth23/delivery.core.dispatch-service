package com.delivery.dispatch.domain;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Rematch decisions after a shipper rejects or ignores the single active offer.
 * Exclusion sources intentionally differ between the two paths to preserve the
 * legacy Saga behavior: rejection rematch excludes only previously rejecting
 * shippers, while offer-timeout rematch excludes every shipper recorded with a
 * rejectedShipperId in the case history (including earlier timeouts).
 */
public final class RematchPolicy {

    /** Shared bound on failed offers before the case compensates as SHIPPER_NOT_FOUND. */
    public static final int MAX_FAILED_OFFERS = 5;

    private RematchPolicy() {
    }

    public sealed interface Decision permits Duplicate, Exhausted, Rematch {
    }

    /** The same shipper rejection was already applied. */
    public record Duplicate() implements Decision {
    }

    /** Offer attempts are exhausted; compensate with SHIPPER_NOT_FOUND. */
    public record Exhausted() implements Decision {
    }

    /**
     * @param attempt       one-based failed-offer ordinal used in the history step name
     * @param excludedShipperIds ordered, distinct shippers Match must skip
     */
    public record Rematch(long attempt, List<Long> excludedShipperIds) implements Decision {
        public Rematch {
            excludedShipperIds = List.copyOf(excludedShipperIds);
        }
    }

    /**
     * @param rejectedShipperId          shipper rejecting now; may be null for a legacy event
     * @param previousRejections         rejecting shippers from earlier rejection steps, history order
     * @param previousRejectionSteps     number of earlier rejection steps
     */
    public static Decision onRejection(Long rejectedShipperId, List<Long> previousRejections,
                                       long previousRejectionSteps) {
        if (rejectedShipperId != null && previousRejections.contains(rejectedShipperId)) {
            return new Duplicate();
        }
        long attempt = previousRejectionSteps + 1;
        if (attempt > MAX_FAILED_OFFERS) {
            return new Exhausted();
        }
        List<Long> excluded = new ArrayList<>();
        if (rejectedShipperId != null) {
            excluded.add(rejectedShipperId);
        }
        for (Long previous : previousRejections) {
            if (!excluded.contains(previous)) {
                excluded.add(previous);
            }
        }
        return new Rematch(attempt, excluded);
    }

    /**
     * @param timedOutShipperId      positive shipper of the expired offer
     * @param recordedRejectedIds    every rejectedShipperId recorded in history, history order
     * @param previousFailedOffers   earlier rejection plus offer-timeout steps
     */
    public static Decision onOfferTimeout(long timedOutShipperId, List<Long> recordedRejectedIds,
                                          long previousFailedOffers) {
        if (timedOutShipperId <= 0) {
            throw new IllegalStateException("Offer payload shipperId must be positive");
        }
        if (previousFailedOffers >= MAX_FAILED_OFFERS) {
            return new Exhausted();
        }
        LinkedHashSet<Long> excluded = new LinkedHashSet<>(recordedRejectedIds);
        excluded.add(timedOutShipperId);
        return new Rematch(previousFailedOffers + 1, new ArrayList<>(excluded));
    }
}
