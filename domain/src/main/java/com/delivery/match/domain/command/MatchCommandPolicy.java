package com.delivery.match.domain.command;

import java.util.Objects;
import java.util.UUID;

/**
 * Durable Match command rules: exact-replay identity, generation ownership,
 * cancellation tombstone identity and the effect of a stop on a command.
 * Persistence, isolation and locking stay in the infrastructure store.
 */
public final class MatchCommandPolicy {

    /** Lifecycle of a durable find command. Names match the persisted values. */
    public enum Status { PENDING, CANDIDATE_STAGED, RESULT_STAGED, CANCELLED }

    /** What the listener does with an accepted find command. */
    public enum Next { PROCESS, RESUME, TERMINAL }

    /** Outbox states a stop may still suppress before relay. */
    public enum OutboxStatus { PENDING, IN_FLIGHT, SENT, DEAD, CANCELLED }

    /** Source identity of a find command; the fingerprint covers the raw payload bytes. */
    public record CommandIdentity(String topic, Long orderId, Long deliveryId,
                                  UUID matchingSessionId, String payloadFingerprint) {
    }

    private MatchCommandPolicy() {
    }

    /** An exact replay resumes incomplete work; a terminal command is never re-run. */
    public static Next next(Status existing) {
        return switch (existing) {
            case PENDING -> Next.PROCESS;
            case CANDIDATE_STAGED -> Next.RESUME;
            case RESULT_STAGED, CANCELLED -> Next.TERMINAL;
        };
    }

    public static void requireExactReplay(CommandIdentity existing, CommandIdentity incoming) {
        if (!existing.equals(incoming)) {
            throw new IllegalArgumentException("Match command eventId replay has a contradictory payload");
        }
    }

    /** A matching generation belongs to exactly one command eventId. */
    public static void requireUnownedGeneration(boolean ownedByAnotherCommand) {
        if (ownedByAnotherCommand) {
            throw new IllegalArgumentException(
                    "Match matchingSessionId is already owned by a different command eventId");
        }
    }

    public static void requireTombstoneIdentity(Long tombstoneOrderId, Long tombstoneDeliveryId,
                                                UUID tombstoneSessionId, Long orderId, Long deliveryId,
                                                UUID matchingSessionId) {
        if (!Objects.equals(tombstoneOrderId, orderId)
                || !Objects.equals(tombstoneDeliveryId, deliveryId)
                || !Objects.equals(tombstoneSessionId, matchingSessionId)) {
            throw new IllegalArgumentException(
                    "Match cancellation tombstone conflicts with order, delivery or matching session identity");
        }
    }

    public static void requireExactStopReplay(String tombstoneFingerprint, String fingerprint) {
        if (!Objects.equals(tombstoneFingerprint, fingerprint)) {
            throw new IllegalArgumentException("stop-matching eventId replay has a contradictory payload");
        }
    }

    public static void requireStopCommand(UUID stopEventId, Long orderId, Long deliveryId,
                                          UUID matchingSessionId, String rawPayload) {
        if (stopEventId == null || orderId == null || orderId <= 0
                || deliveryId == null || deliveryId <= 0 || matchingSessionId == null) {
            throw new IllegalArgumentException(
                    "stop-matching eventId, orderId, deliveryId and matchingSessionId are required");
        }
        if (rawPayload == null || rawPayload.isBlank()) {
            throw new IllegalArgumentException("raw payload is required");
        }
    }

    /** Only results not yet sent to Kafka can still be suppressed. */
    public static boolean suppressible(OutboxStatus status) {
        return status == OutboxStatus.PENDING || status == OutboxStatus.DEAD;
    }

    /**
     * A stop cancels a command unless it is already cancelled or its result
     * was already relayed (no result remained suppressible).
     */
    public static boolean cancels(Status status, boolean suppressedUnsentResult) {
        if (status == Status.CANCELLED) {
            return false;
        }
        return status != Status.RESULT_STAGED || suppressedUnsentResult;
    }
}
