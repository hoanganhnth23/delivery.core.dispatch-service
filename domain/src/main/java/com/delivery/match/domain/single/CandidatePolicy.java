package com.delivery.match.domain.single;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.ArrayList;
import java.util.Collections;

/** Deterministic exclusion, active profile selection and ranking, independent of host DTOs. */
public final class CandidatePolicy {
    public static final Profile NEAREST_COD = new Profile("nearest-cod", "v1");
    public static final Profile BALANCED_ETA = new Profile("balanced-eta", "v1");
    private CandidatePolicy() { }

    public record Profile(String id, String version) { }
    public record Candidate(Long shipperId, double distanceKm, long completedDeliveries) { }
    public record RankedCandidate(Candidate candidate, Double score) { }

    public enum SelectionStatus { AVAILABLE, NO_CANDIDATES, ALL_EXCLUDED }

    public record ExclusionInput(List<Long> shipperIds, List<Long> excludedIds) {
        public ExclusionInput {
            shipperIds = immutableIds(shipperIds);
            excludedIds = immutableIds(excludedIds);
        }
    }

    public record Selection(List<Integer> availableIndexes, List<Long> rejectedIds,
                            SelectionStatus status, boolean exclusionApplied) {
        public Selection {
            availableIndexes = List.copyOf(availableIndexes);
            rejectedIds = immutableIds(rejectedIds);
        }
    }

    // Null identities were accepted by the old exclusion list; preserve contains(null) semantics.
    private static List<Long> immutableIds(List<Long> ids) {
        return ids == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(ids));
    }

    public static Selection exclude(ExclusionInput input) {
        List<Integer> available = new ArrayList<>();
        List<Long> rejected = new ArrayList<>();
        for (int index = 0; index < input.shipperIds().size(); index++) {
            Long id = input.shipperIds().get(index);
            if (input.excludedIds().contains(id)) rejected.add(id);
            else available.add(index);
        }
        SelectionStatus status = input.shipperIds().isEmpty() ? SelectionStatus.NO_CANDIDATES
                : available.isEmpty() ? SelectionStatus.ALL_EXCLUDED : SelectionStatus.AVAILABLE;
        return new Selection(available, rejected, status,
                !input.shipperIds().isEmpty() && !input.excludedIds().isEmpty());
    }

    public static Profile select(boolean enabled, int canaryPercent, UUID eventId) {
        int percent = Math.max(0, Math.min(100, canaryPercent));
        if (!enabled || percent == 0 || eventId == null) return NEAREST_COD;
        if (percent == 100 || Math.floorMod(eventId.hashCode(), 100) < percent) return BALANCED_ETA;
        return NEAREST_COD;
    }

    public static List<RankedCandidate> rank(Profile profile, List<Candidate> candidates, double speed, double penalty) {
        if (candidates == null || candidates.isEmpty()) return List.of();
        if (!BALANCED_ETA.equals(profile)) {
            return candidates.stream().map(candidate -> new RankedCandidate(candidate, null)).toList();
        }
        if (!Double.isFinite(speed) || speed <= 0) {
            throw new IllegalStateException("balanced-eta requires a positive ETA speed");
        }
        if (!Double.isFinite(penalty) || penalty < 0) {
            throw new IllegalStateException("balanced-eta fairness penalty must be non-negative");
        }
        return candidates.stream().map(candidate -> new RankedCandidate(candidate,
                        BigDecimal.valueOf(candidate.distanceKm() / speed
                                + Math.max(0L, candidate.completedDeliveries()) * penalty)
                                .setScale(4, RoundingMode.HALF_UP).doubleValue()))
                .sorted(Comparator.comparing(RankedCandidate::score)
                        .thenComparing(r -> r.candidate().distanceKm())
                        .thenComparing(r -> r.candidate().shipperId()))
                .toList();
    }
}
