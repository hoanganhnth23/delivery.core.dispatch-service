package com.delivery.dispatch.domain;

/** Retry budget sent to Match with each find-shipper command. */
public record MatchingRetrySettings(int maxRetryAttempts, int initialDelaySeconds,
                                    int maxDelaySeconds, double backoffMultiplier) {

    /** Fixed budget for rematching after a rejection or an offer timeout. */
    public static final MatchingRetrySettings REMATCH = new MatchingRetrySettings(5, 15, 120, 1.5);
}
