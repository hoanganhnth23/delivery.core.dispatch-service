package com.delivery.dispatch.domain;

import java.time.Duration;
import java.time.LocalDateTime;

/** Retry count excludes the original command; each resend gets a reply window. */
public final class StuckStateTimeoutPolicy {
    private StuckStateTimeoutPolicy() { }
    public enum Decision { WAIT, RESEND, FAIL }

    public static Decision decide(DispatchStatus state, LocalDateTime enteredAt, int attempts,
                                  LocalDateTime now, Duration timeout, int maxResends) {
        if (state != DispatchStatus.OFFER_PERSISTING && state != DispatchStatus.COMPENSATING
                && state != DispatchStatus.OFFER_RETIRING) return Decision.WAIT;
        if (enteredAt == null || now.isBefore(enteredAt.plus(timeout))) return Decision.WAIT;
        return attempts >= maxResends ? Decision.FAIL : Decision.RESEND;
    }
}
