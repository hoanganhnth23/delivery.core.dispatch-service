package com.delivery.dispatch.domain;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Typed read of a coordination case's append-only fact history. The
 * persisted representation (legacy saga_steps JSON) stays in the adapter;
 * decisions consume only these facts.
 */
public interface CaseHistory {

    /** One recorded fact: its raw event data and when it was recorded. */
    record Fact(String eventData, LocalDateTime executedAt) {
    }

    boolean has(String stepName);

    /** Event data of the most recent step with exactly this name, or null. */
    String latest(String stepName);

    /** Event data of the most recent step whose name starts with the prefix, or null. */
    String latestWithPrefix(String stepPrefix);

    /** Most recent step with exactly this name (even without event data), or null. */
    Fact latestFact(String stepName);

    /**
     * Text of one field in the latest step with this name; null when the step,
     * its data or the field is missing, or the data is unreadable.
     */
    String latestField(String stepName, String field);

    long countWithPrefix(String stepPrefix);

    long count(String stepName);

    /** Shippers recorded by rejection steps, history order (duplicates kept). */
    List<Long> rejectingShippers();

    /** Every rejectedShipperId recorded on any step, history order (duplicates kept). */
    List<Long> recordedRejectedShippers();

    /**
     * Session of the latest matching generation, or null when matching never
     * started or the case predates generation-aware matching.
     *
     * @throws IllegalStateException when the persisted session is malformed
     */
    UUID currentMatchingSession();
}
