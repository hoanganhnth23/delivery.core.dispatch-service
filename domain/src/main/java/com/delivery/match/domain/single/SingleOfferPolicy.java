package com.delivery.match.domain.single;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

/** Stable outcome identity and the existing single-offer limits. */
public final class SingleOfferPolicy {
    public static final int WAITING_TIMEOUT_SECONDS = 180;
    public static final double SEARCH_RADIUS_KM = 5.0;
    private SingleOfferPolicy() { }

    public static UUID sessionId(UUID commandId, UUID sessionId) {
        return sessionId == null ? commandId : sessionId;
    }

    public static UUID outcomeId(String outcome, UUID commandId) {
        if (outcome == null || outcome.isBlank() || commandId == null) {
            throw new IllegalArgumentException("Match outcome and command eventId are required");
        }
        return UUID.nameUUIDFromBytes(("match:" + outcome + ":" + commandId).getBytes(StandardCharsets.UTF_8));
    }

    public record Candidate(Long shipperId, String name, String phone, double distanceKm,
                            double latitude, double longitude, boolean online) { }

    public static List<Candidate> offer(List<Candidate> candidates) {
        return candidates.stream().limit(1).toList();
    }

    public static boolean codEligible(Boolean eligible) {
        return Boolean.TRUE.equals(eligible);
    }
}
