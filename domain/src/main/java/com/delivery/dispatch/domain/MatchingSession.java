package com.delivery.dispatch.domain;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Deterministic matching generation identity. Each find-shipper command of one
 * coordination case gets a stable name-based UUID, independent of outbox IDs.
 */
public final class MatchingSession {

    private MatchingSession() {
    }

    public static String caseIdentity(UUID caseId, long orderId) {
        return caseId == null ? "order:" + orderId : caseId.toString();
    }

    /**
     * @param caseIdentity      value of {@link #caseIdentity(UUID, long)}
     * @param startedGenerations number of matching generations already started
     */
    public static UUID next(String caseIdentity, long startedGenerations) {
        if (caseIdentity == null || caseIdentity.isBlank() || startedGenerations < 0) {
            throw new IllegalArgumentException("Matching session identity requires a case and generation");
        }
        long generation = startedGenerations + 1L;
        return UUID.nameUUIDFromBytes(("saga:matching-session:" + caseIdentity + ":" + generation)
                .getBytes(StandardCharsets.UTF_8));
    }

    /**
     * A case started before generation-aware matching has no expected session;
     * any result is accepted for in-flight compatibility.
     */
    public static boolean isCurrent(UUID expected, UUID actual) {
        if (expected == null) {
            return true;
        }
        if (actual == null) {
            throw new IllegalArgumentException(
                    "Match result matchingSessionId is required for a generation-aware Saga");
        }
        return expected.equals(actual);
    }
}
