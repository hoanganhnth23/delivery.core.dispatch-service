package com.delivery.dispatch.domain;

/**
 * Delivery's answer to an expire-shipper-offer command. The coordinator waits
 * in OFFER_RETIRING for this acknowledgement before rematching, so an
 * acceptance committed near the deadline is never raced by a new offer.
 */
public final class OfferRetirementPolicy {

    public enum Decision {
        /** The offer was cleared; start the prepared rematch generation. */
        REMATCH,
        /** Delivery had already committed the shipper's acceptance; converge to it. */
        ASSIGN,
        /** Delivery is cancelled or terminally unmatched; its own terminal fact drives the case. */
        TERMINAL
    }

    private OfferRetirementPolicy() {
    }

    public static Decision decide(String outcome, Long shipperId) {
        if (outcome == null) {
            throw new IllegalArgumentException("Offer retirement outcome is required");
        }
        return switch (outcome) {
            case "RETIRED" -> Decision.REMATCH;
            case "ASSIGNED" -> {
                if (shipperId == null || shipperId <= 0) {
                    throw new IllegalArgumentException("Assigned offer retirement requires a positive shipperId");
                }
                yield Decision.ASSIGN;
            }
            case "TERMINAL" -> Decision.TERMINAL;
            default -> throw new IllegalArgumentException("Unsupported offer retirement outcome: " + outcome);
        };
    }
}
