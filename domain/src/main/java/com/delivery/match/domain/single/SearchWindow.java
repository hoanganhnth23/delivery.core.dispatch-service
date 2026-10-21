package com.delivery.match.domain.single;

import java.time.LocalDateTime;
import java.util.function.Supplier;

/** Absolute Saga deadline; retries must finish their delay strictly before the cutoff. */
public final class SearchWindow {
    private SearchWindow() { }

    public static boolean deadlineReached(LocalDateTime deadline, LocalDateTime now) {
        return deadlineReached(deadline, () -> now);
    }

    public static boolean canRetry(LocalDateTime deadline, LocalDateTime now, long delayMs) {
        return canRetry(deadline, () -> now, delayMs);
    }
    public static boolean deadlineReached(LocalDateTime deadline, Supplier<LocalDateTime> now) {
        return deadline != null && !deadline.isAfter(now.get());
    }

    public static boolean canRetry(LocalDateTime deadline, Supplier<LocalDateTime> now, long delayMs) {
        return deadline == null || now.get().plusNanos(delayMs * 1_000_000L).isBefore(deadline);
    }
}

