package com.delivery.match.domain.batch;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Lifecycle rules of one batch pool item (one delivery generation waiting for
 * a rolling round): intake, round admission, requeue, deadline expiry and
 * retirement after Delivery releases or completes a batch.
 */
public final class BatchPoolPolicy {

    /** Names match the persisted pool item states. */
    public enum State { WAITING, CLAIMED, ASSIGNED, REQUEUED, EXPIRED, CANCELLED }

    /** Matching window used when a command carries no absolute deadline. */
    public static final int DEFAULT_DEADLINE_MINUTES = 5;

    public static final String EXPIRY_REASON = "Matching deadline expired before a batch shipper was assigned";

    private BatchPoolPolicy() {
    }

    public static void requireIntake(Long orderId, Long deliveryId, UUID matchingSessionId, boolean batchEnabled) {
        if (deliveryId == null || deliveryId <= 0 || orderId == null || orderId <= 0 || matchingSessionId == null) {
            throw new IllegalArgumentException("Valid order, delivery and matching session are required");
        }
        if (!batchEnabled) {
            throw new IllegalStateException("Rolling batch dispatch is disabled");
        }
    }

    public static LocalDateTime deadline(LocalDateTime now, LocalDateTime requestedDeadline) {
        return requestedDeadline == null ? now.plusMinutes(DEFAULT_DEADLINE_MINUTES) : requestedDeadline;
    }

    public static int initialWave(Integer batchWave) {
        return batchWave == null ? 0 : Math.max(0, batchWave);
    }

    /** Decision for a claimed item when its round starts. */
    public enum Admission { ADMIT, CANCEL, RETURN_FOR_EXPIRY }

    /**
     * A stopped generation is cancelled; an item past its absolute deadline
     * returns to WAITING so the expiry sweep stages its deterministic not-found.
     */
    public static Admission admission(boolean generationCancelled, LocalDateTime deadline, LocalDateTime now) {
        if (generationCancelled) return Admission.CANCEL;
        if (!deadline.isAfter(now)) return Admission.RETURN_FOR_EXPIRY;
        return Admission.ADMIT;
    }

    /** Unassigned items go back to the pool until their wave budget is spent. */
    public static State requeueState(int waveNumber, int maxWaves) {
        return waveNumber >= Math.max(1, maxWaves) ? State.EXPIRED : State.WAITING;
    }

    /** Only a WAITING item whose deadline has passed is expired by the sweep. */
    public static boolean expires(State state, LocalDateTime deadline, LocalDateTime now) {
        return state == State.WAITING && deadline != null && !deadline.isAfter(now);
    }

    /** A stop retires items that have not yet been offered in a batch. */
    public static boolean retiredByStop(State state) {
        return state == State.WAITING || state == State.CLAIMED;
    }

    /** A released/completed batch retires the old generation unless it is already final. */
    public static boolean retiredByBatchRelease(State state) {
        return state != State.CANCELLED && state != State.EXPIRED;
    }
}
