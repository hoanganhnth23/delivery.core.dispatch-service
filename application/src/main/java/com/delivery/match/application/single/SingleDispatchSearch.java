package com.delivery.match.application.single;

import com.delivery.match.domain.single.SearchWindow;
import com.delivery.match.domain.single.SingleOfferPolicy;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.UUID;

/** Queries cancellation for the command generation and samples the clock at each search checkpoint. */
public final class SingleDispatchSearch {
    @FunctionalInterface
    public interface CancellationLookup {
        boolean isCancelled(Long deliveryId, UUID sessionId);
    }

    private final CancellationLookup cancellation;
    private final Clock clock;

    public SingleDispatchSearch(CancellationLookup cancellation, Clock clock) {
        this.cancellation = cancellation;
        this.clock = clock;
    }

    public boolean isCancelled(Long deliveryId, UUID commandId, UUID sessionId) {
        return cancellation.isCancelled(deliveryId, SingleOfferPolicy.sessionId(commandId, sessionId));
    }

    public boolean deadlineReached(LocalDateTime deadline) {
        return SearchWindow.deadlineReached(deadline, () -> LocalDateTime.now(clock));
    }

    public boolean canRetry(LocalDateTime deadline, long delayMs) {
        return SearchWindow.canRetry(deadline, () -> LocalDateTime.now(clock), delayMs);
    }
}
