package com.delivery.dispatch.application;

import com.delivery.dispatch.domain.DispatchStatus;
import com.delivery.dispatch.domain.StuckStateTimeoutPolicy;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.function.IntConsumer;

/** Called with a locked case; command requeue and retry bookkeeping share its transaction. */
public final class DefaultStuckStateTimeoutUseCase {
    public StuckStateTimeoutPolicy.Decision expire(DispatchStatus state, LocalDateTime enteredAt,
            int attempts, LocalDateTime now, Duration timeout, int maxResends,
            Runnable resend, IntConsumer recordResend, Runnable fail) {
        var decision = StuckStateTimeoutPolicy.decide(state, enteredAt, attempts, now, timeout, maxResends);
        switch (decision) {
            case RESEND -> { resend.run(); recordResend.accept(attempts + 1); }
            case FAIL -> fail.run();
            case WAIT -> { }
        }
        return decision;
    }
}
