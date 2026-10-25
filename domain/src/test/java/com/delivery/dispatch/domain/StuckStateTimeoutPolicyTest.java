package com.delivery.dispatch.domain;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.time.LocalDateTime;
import static org.junit.jupiter.api.Assertions.assertEquals;

class StuckStateTimeoutPolicyTest {
    @Test
    void allStatesRespectDeadlineAndRetryBudget() {
        var entered = LocalDateTime.of(2026, 10, 6, 0, 0);
        var timeout = Duration.ofSeconds(120);
        for (var state : DispatchStatus.values()) {
            boolean awaited = state == DispatchStatus.OFFER_PERSISTING
                    || state == DispatchStatus.COMPENSATING || state == DispatchStatus.OFFER_RETIRING;
            assertEquals(StuckStateTimeoutPolicy.Decision.WAIT,
                    StuckStateTimeoutPolicy.decide(state, entered, 0, entered.plusSeconds(119), timeout, 3));
            assertEquals(awaited ? StuckStateTimeoutPolicy.Decision.RESEND : StuckStateTimeoutPolicy.Decision.WAIT,
                    StuckStateTimeoutPolicy.decide(state, entered, 2, entered.plusSeconds(120), timeout, 3));
            assertEquals(awaited ? StuckStateTimeoutPolicy.Decision.FAIL : StuckStateTimeoutPolicy.Decision.WAIT,
                    StuckStateTimeoutPolicy.decide(state, entered, 3, entered.plusSeconds(120), timeout, 3));
        }
        assertEquals(StuckStateTimeoutPolicy.Decision.WAIT, StuckStateTimeoutPolicy.decide(
                DispatchStatus.OFFER_PERSISTING, null, 0, entered, timeout, 3));
        assertEquals(StuckStateTimeoutPolicy.Decision.FAIL, StuckStateTimeoutPolicy.decide(
                DispatchStatus.OFFER_RETIRING, entered, 0, entered.plusSeconds(120), timeout, 0));
    }
}
