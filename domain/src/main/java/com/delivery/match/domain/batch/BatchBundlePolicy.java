package com.delivery.match.domain.batch;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.ToDoubleFunction;

/**
 * Framework-free batch dispatch rules: pool item admission, pickup
 * feasibility, bounded bundle enumeration, detour/score policy, deterministic
 * identities and the emitted stop sequence. Values mirror the locked
 * production-matching-v1 decisions; adapters supply GEO, COD and routing facts.
 */
public final class BatchBundlePolicy {

    /** Nearby-shipper search radius around each pickup. */
    public static final double SEARCH_RADIUS_KM = 5.0;
    /** Maximum pairwise pickup distance inside one bundle. */
    public static final double MAX_PICKUP_SPREAD_KM = 2.0;
    /** Maximum orders in one bundle. */
    public static final int MAX_BUNDLE_SIZE = 3;
    /** COD hold and Redis batch offer lifetime. */
    public static final int OFFER_TTL_SECONDS = 180;

    private static final double EARTH_RADIUS_KM = 6371.0;

    private BatchBundlePolicy() {
    }

    /** Coordinate pair; either value may be absent in legacy rows. */
    public record Point(Double latitude, Double longitude) {
    }

    /** A pool item may enter a round only within its wave budget, as COD, with full coordinates. */
    public static boolean admits(int waveNumber, int maxWaves, String paymentMethod,
                                 Point pickup, Point dropoff) {
        if (waveNumber >= Math.max(1, maxWaves)) return false;
        return "COD".equalsIgnoreCase(paymentMethod)
                && pickup.latitude() != null && pickup.longitude() != null
                && dropoff.latitude() != null && dropoff.longitude() != null;
    }

    /** Haversine distance; an unknown coordinate is infinitely far. */
    public static double distanceKm(Point a, Point b) {
        if (a.latitude() == null || a.longitude() == null || b.latitude() == null || b.longitude() == null) {
            return Double.MAX_VALUE;
        }
        double dLat = Math.toRadians(b.latitude() - a.latitude());
        double dLng = Math.toRadians(b.longitude() - a.longitude());
        double h = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(a.latitude())) * Math.cos(Math.toRadians(b.latitude()))
                * Math.sin(dLng / 2) * Math.sin(dLng / 2);
        return EARTH_RADIUS_KM * 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
    }

    /** Every pair of pickups in the bundle must lie within {@link #MAX_PICKUP_SPREAD_KM}. */
    public static boolean pickupsFeasible(List<Point> pickups) {
        for (int i = 0; i < pickups.size(); i++) {
            for (int j = i + 1; j < pickups.size(); j++) {
                if (distanceKm(pickups.get(i), pickups.get(j)) > MAX_PICKUP_SPREAD_KM) return false;
            }
        }
        return true;
    }

    /** Nearest pickups first (ties by identity), bounded by the per-shipper seed budget. */
    public static List<UUID> seeds(List<UUID> orderIds, ToDoubleFunction<UUID> distanceFromShipper, int limit) {
        return orderIds.stream()
                .sorted(Comparator.comparingDouble(distanceFromShipper).thenComparing(UUID::toString))
                .limit(Math.max(1, limit))
                .toList();
    }

    /**
     * Enumerates bundles of size 1..3 in deterministic order: singletons from
     * every reachable order, pairs and triples only from the seeds.
     */
    public static void bundles(List<UUID> sortedOrderIds, List<UUID> seedOrderIds, Consumer<List<UUID>> consumer) {
        for (int size = 1; size <= Math.min(MAX_BUNDLE_SIZE, sortedOrderIds.size()); size++) {
            combinations(size == 1 ? sortedOrderIds : seedOrderIds, size, 0, new ArrayList<>(), consumer);
        }
    }

    private static void combinations(List<UUID> values, int size, int start, List<UUID> current,
                                     Consumer<List<UUID>> consumer) {
        if (current.size() == size) {
            consumer.accept(List.copyOf(current));
            return;
        }
        for (int i = start; i <= values.size() - (size - current.size()); i++) {
            current.add(values.get(i));
            combinations(values, size, i + 1, current, consumer);
            current.remove(current.size() - 1);
        }
    }

    /** Extra route time over the best single-order route; never negative. */
    public static long incrementalSeconds(long routeSeconds, long bestSoloSeconds) {
        return Math.max(0, routeSeconds - bestSoloSeconds);
    }

    public static boolean withinDetour(long incrementalSeconds, long maxEtaDetourSeconds) {
        return incrementalSeconds <= maxEtaDetourSeconds;
    }

    /** Lower is better: route duration dominates, detour breaks ties. */
    public static long score(long routeSeconds, long incrementalSeconds) {
        return routeSeconds * 1_000L + incrementalSeconds * 100L;
    }

    public static UUID bundleId(long shipperId, List<UUID> orderIds) {
        return name("bundle:" + shipperId + ":" + orderIds);
    }

    public static UUID batchId(UUID roundId, long shipperId, UUID bundleId) {
        return name("dispatch-batch:" + roundId + ":" + shipperId + ":" + bundleId);
    }

    public static UUID codHoldId(UUID batchId, long deliveryId) {
        return name("cod-hold:" + batchId + ":" + deliveryId);
    }

    public static UUID codOfferId(UUID batchId, long deliveryId) {
        return name("cod-offer:" + batchId + ":" + deliveryId);
    }

    public static UUID shipperFoundEventId(UUID batchId, long deliveryId) {
        return name("shipper-found:" + batchId + ":" + deliveryId);
    }

    /**
     * Global stop positions 0..(2n-1) for the scored order: each pickup is
     * immediately followed by its drop-off before the next order's pickup.
     */
    public record StopSequence(int pickup, int dropoff) {
    }

    public static StopSequence stopSequence(int index, int itemCount) {
        if (index < 0 || index >= itemCount) {
            throw new IllegalArgumentException("Batch item index is outside the bundle");
        }
        return new StopSequence(2 * index, 2 * index + 1);
    }

    /** Offer response window clamped to 1..180 seconds. */
    public static int waitingTimeoutSeconds(int waveTimeoutSeconds) {
        return Math.max(1, Math.min(waveTimeoutSeconds, OFFER_TTL_SECONDS));
    }

    private static UUID name(String value) {
        return UUID.nameUUIDFromBytes(value.getBytes(StandardCharsets.UTF_8));
    }
}
