package com.delivery.match.application.single;

import org.junit.jupiter.api.Test;
import java.time.*;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class SingleDispatchSearchTest {
    @Test void checksGenerationCancellationAndAbsoluteClockAtEveryInvocation() {
        UUID command = new UUID(0, 1), session = new UUID(0, 2);
        var clock = Clock.fixed(Instant.parse("2026-10-04T00:00:00Z"), ZoneOffset.ofHours(7));
        var search = new SingleDispatchSearch((delivery, generation) -> {
            assertEquals(10L, delivery);
            return session.equals(generation);
        }, clock);
        assertTrue(search.isCancelled(10L, command, session));
        assertFalse(search.isCancelled(10L, command, null));
        LocalDateTime now = LocalDateTime.now(clock);
        assertTrue(search.deadlineReached(now));
        assertFalse(search.deadlineReached(null));
        assertTrue(search.canRetry(now.plusSeconds(31), 30000));
        assertFalse(search.canRetry(now.plusSeconds(30), 30000));
    }
}
