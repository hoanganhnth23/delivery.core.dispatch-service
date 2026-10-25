package com.delivery.dispatch.application;

import com.delivery.dispatch.domain.DispatchStatus;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultStuckStateTimeoutUseCaseTest {
    @Test
    void resendsThenWaitsForLastReplyBeforeFailing() {
        var useCase = new DefaultStuckStateTimeoutUseCase();
        var entered = LocalDateTime.now();
        List<String> actions = new ArrayList<>();
        for (int attempts = 0; attempts <= 3; attempts++) {
            useCase.expire(DispatchStatus.COMPENSATING, entered, attempts, entered.plusSeconds(120),
                    Duration.ofSeconds(120), 3, () -> actions.add("resend"),
                    count -> actions.add("attempt:" + count), () -> actions.add("fail"));
        }
        assertThat(actions).containsExactly("resend", "attempt:1", "resend", "attempt:2",
                "resend", "attempt:3", "fail");
        actions.clear();
        useCase.expire(DispatchStatus.COMPENSATING, entered, 3, entered.plusSeconds(119),
                Duration.ofSeconds(120), 3, () -> actions.add("resend"),
                count -> actions.add("attempt"), () -> actions.add("fail"));
        assertThat(actions).isEmpty();
    }

    @Test
    void failedRequeueDoesNotConsumeRetryBudget() {
        List<Integer> attempts = new ArrayList<>();
        var entered = LocalDateTime.now();
        assertThatThrownBy(() -> new DefaultStuckStateTimeoutUseCase().expire(
                DispatchStatus.OFFER_PERSISTING, entered, 0, entered.plusSeconds(120), Duration.ofSeconds(120), 3,
                () -> { throw new IllegalStateException("outbox unavailable"); }, attempts::add,
                () -> { throw new AssertionError("must not fail case"); })).isInstanceOf(IllegalStateException.class);
        assertThat(attempts).isEmpty();
    }
}
