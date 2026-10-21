package com.delivery.match.application;

import com.delivery.match.application.api.DispatchMatchingPort;
import com.delivery.match.domain.dispatch.DispatchBundleCandidate;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Framework-free dispatch assignment use case. */
public final class DefaultDispatchMatchingService implements DispatchMatchingPort {
    private static final long COVERAGE_BONUS = 1_000_000L;

    @Override
    public List<DispatchBundleCandidate> optimize(List<DispatchBundleCandidate> candidates, int maxAssignments) {
        if (candidates == null || candidates.isEmpty() || maxAssignments <= 0) return List.of();

        Comparator<DispatchBundleCandidate> quality = Comparator
                .comparingInt(DispatchBundleCandidate::coveredOrders).reversed()
                .thenComparingLong(DispatchBundleCandidate::scoreMicros)
                .thenComparingLong(DispatchBundleCandidate::routeSeconds)
                .thenComparing(DispatchBundleCandidate::bundleId);
        List<DispatchBundleCandidate> ordered = candidates.stream()
                .filter(candidate -> candidate.poolItemIds().size() <= 3)
                .sorted(Comparator.comparingLong(this::flowCost).thenComparing(DispatchBundleCandidate::bundleId))
                .toList();

        List<DispatchBundleCandidate> selected = new ArrayList<>();
        Set<Long> shippers = new HashSet<>();
        Set<java.util.UUID> orders = new HashSet<>();
        while (selected.size() < maxAssignments) {
            DispatchBundleCandidate next = ordered.stream()
                    .filter(candidate -> !shippers.contains(candidate.shipperId()))
                    .filter(candidate -> candidate.poolItemIds().stream().noneMatch(orders::contains))
                    .min(quality)
                    .orElse(null);
            if (next == null) break;
            selected.add(next);
            shippers.add(next.shipperId());
            orders.addAll(next.poolItemIds());
        }
        return selected.stream().sorted(quality).toList();
    }

    private long flowCost(DispatchBundleCandidate candidate) {
        return candidate.scoreMicros() - COVERAGE_BONUS * candidate.coveredOrders();
    }
}
