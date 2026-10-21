package com.delivery.match_service.service;

import com.delivery.match_service.entity.DispatchPoolItem;
import com.delivery.routing.contracts.Coordinate;
import com.delivery.routing.contracts.RouteRequest;
import com.delivery.routing.contracts.RouteResponse;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/** Calls routing-service for bounded ETA ranking and falls back to geodesic locally. */
@Service
public class RoutingClient {
    private final com.delivery.routing.client.RoutingClient routingClient;
    private final Map<String, CachedLeg> legCache = new ConcurrentHashMap<>();
    private static final long LEG_CACHE_TTL_NANOS = Duration.ofSeconds(5).toNanos();
    private static final int MAX_CACHED_LEGS = 4096;

    public RoutingClient(com.delivery.routing.client.RoutingClient routingClient) {
        this.routingClient = routingClient;
    }

    public long estimateRouteSeconds(double originLat, double originLng, List<DispatchPoolItem> items) {
        return planRoute(originLat, originLng, items).durationSeconds();
    }

    /**
     * Finds the best bounded order for a batch. Batch size is capped at three,
     * so evaluating every permutation is deterministic and cheap while still
     * preserving the invariant that each pickup precedes its own drop-off.
     */
    public RoutePlan planRoute(double originLat, double originLng, List<DispatchPoolItem> items) {
        if (items == null || items.isEmpty()) return new RoutePlan(List.of(), 0);
        List<DispatchPoolItem> normalized = items.stream()
                .sorted(Comparator.comparing(item -> item.getPoolItemId().toString()))
                .toList();
        RoutePlan[] best = new RoutePlan[1];
        permute(originLat, originLng, normalized, new ArrayList<>(), new boolean[normalized.size()], best);
        return best[0] == null ? new RoutePlan(normalized, routeSeconds(originLat, originLng, normalized)) : best[0];
    }

    private void permute(double originLat, double originLng, List<DispatchPoolItem> items,
                         List<DispatchPoolItem> current, boolean[] used, RoutePlan[] best) {
        if (current.size() == items.size()) {
            long duration = routeSeconds(originLat, originLng, current);
            RoutePlan candidate = new RoutePlan(current, duration);
            if (best[0] == null || duration < best[0].durationSeconds()
                    || (duration == best[0].durationSeconds()
                    && candidate.key().compareTo(best[0].key()) < 0)) best[0] = candidate;
            return;
        }
        for (int index = 0; index < items.size(); index++) {
            if (used[index]) continue;
            used[index] = true;
            current.add(items.get(index));
            permute(originLat, originLng, items, current, used, best);
            current.remove(current.size() - 1);
            used[index] = false;
        }
    }

    private long routeSeconds(double originLat, double originLng, List<DispatchPoolItem> items) {
        double lat = originLat;
        double lng = originLng;
        long total = 0;
        for (DispatchPoolItem item : items) {
            LegResult pickup = route(lat, lng, item.getPickupLat(), item.getPickupLng());
            LegResult dropoff = route(item.getPickupLat(), item.getPickupLng(), item.getDeliveryLat(), item.getDeliveryLng());
            total = saturatingAdd(total, pickup.durationSeconds());
            total = saturatingAdd(total, dropoff.durationSeconds());
            lat = item.getDeliveryLat();
            lng = item.getDeliveryLng();
        }
        return total;
    }

    private long saturatingAdd(long left, long right) {
        if (right <= 0) return left;
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private LegResult route(double originLat, double originLng, Double destinationLat, Double destinationLng) {
        if (destinationLat == null || destinationLng == null) return new LegResult(Long.MAX_VALUE / 4);
        String cacheKey = String.format(java.util.Locale.ROOT, "%.5f,%.5f>%.5f,%.5f",
                originLat, originLng, destinationLat, destinationLng);
        long now = System.nanoTime();
        CachedLeg cached = legCache.get(cacheKey);
        if (cached != null && now - cached.createdAtNanos() < LEG_CACHE_TTL_NANOS) {
            return new LegResult(cached.durationSeconds());
        }
        long duration = -1;
        try {
            RouteResponse response = routingClient.getRoute(new RouteRequest(
                    "driving-traffic",
                    new Coordinate(originLat, originLng),
                    new Coordinate(destinationLat, destinationLng),
                    null,
                    false));
            if (response != null && response.durationSeconds() >= 0) {
                duration = response.durationSeconds();
            }
        } catch (RuntimeException ignored) {
            // Provider outage must not prevent the bounded dispatch round.
        }
        if (duration < 0) duration = geodesicSeconds(originLat, originLng, destinationLat, destinationLng);
        if (legCache.size() >= MAX_CACHED_LEGS) legCache.clear();
        legCache.put(cacheKey, new CachedLeg(duration, now));
        return new LegResult(duration);
    }

    private long geodesicSeconds(double aLat, double aLng, double bLat, double bLng) {
        double dLat = Math.toRadians(bLat - aLat), dLng = Math.toRadians(bLng - aLng);
        double h = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(aLat)) * Math.cos(Math.toRadians(bLat))
                * Math.sin(dLng / 2) * Math.sin(dLng / 2);
        double km = 6371.0 * 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
        return Math.max(1, Math.round(km / 0.35 * 3600.0));
    }

    private record LegResult(long durationSeconds) { }
    private record CachedLeg(long durationSeconds, long createdAtNanos) { }

    public record RoutePlan(List<DispatchPoolItem> orderedItems, long durationSeconds) {
        public RoutePlan {
            orderedItems = List.copyOf(orderedItems == null ? List.of() : orderedItems);
        }

        private String key() {
            return orderedItems.stream().map(item -> item.getPoolItemId().toString()).reduce("", (left, right) -> left + right);
        }
    }
}
