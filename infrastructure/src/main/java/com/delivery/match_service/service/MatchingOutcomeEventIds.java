package com.delivery.match_service.service;

import java.util.UUID;

/**
 * Stable identities for Match business outcomes.  The command event is the
 * durable generation identity, so a retry or a scheduler replay must derive
 * the same result event rather than append another terminal outcome.
 */
public final class MatchingOutcomeEventIds {

    private MatchingOutcomeEventIds() {
    }

    public static UUID forCommandOutcome(String outcome, UUID commandEventId) {
        return com.delivery.match.domain.single.SingleOfferPolicy.outcomeId(outcome, commandEventId);
    }
}
