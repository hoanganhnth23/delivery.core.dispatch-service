package com.delivery.match.domain.dispatch;

import java.util.HashSet;
import java.util.List;
import java.util.UUID;

/** Immutable candidate for assigning one shipper a bounded delivery bundle. */
public record DispatchBundleCandidate(
        UUID bundleId,
        Long shipperId,
        List<UUID> poolItemIds,
        List<UUID> orderedPoolItemIds,
        long routeSeconds,
        long incrementalEtaSeconds,
        long scoreMicros) {

    public DispatchBundleCandidate(UUID bundleId, Long shipperId, List<UUID> poolItemIds,
                                   long routeSeconds, long incrementalEtaSeconds, long scoreMicros) {
        this(bundleId, shipperId, poolItemIds, poolItemIds,
                routeSeconds, incrementalEtaSeconds, scoreMicros);
    }

    public DispatchBundleCandidate {
        if (bundleId == null || shipperId == null || poolItemIds == null || poolItemIds.isEmpty()) {
            throw new IllegalArgumentException("Bundle identity, shipper and at least one order are required");
        }
        poolItemIds = List.copyOf(poolItemIds);
        if (orderedPoolItemIds == null || orderedPoolItemIds.size() != poolItemIds.size()
                || !new HashSet<>(orderedPoolItemIds).equals(new HashSet<>(poolItemIds))
                || new HashSet<>(orderedPoolItemIds).size() != orderedPoolItemIds.size()) {
            throw new IllegalArgumentException("Route order must contain exactly the bundle orders");
        }
        orderedPoolItemIds = List.copyOf(orderedPoolItemIds);
        if (poolItemIds.size() > 3) {
            throw new IllegalArgumentException("A dispatch batch cannot contain more than three orders");
        }
        if (routeSeconds < 0 || incrementalEtaSeconds < 0 || scoreMicros < 0) {
            throw new IllegalArgumentException("Route and score values must be non-negative");
        }
    }

    public int coveredOrders() {
        return poolItemIds.size();
    }
}
