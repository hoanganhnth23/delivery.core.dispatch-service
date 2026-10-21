package com.delivery.match_service.repository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.GeoOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import com.delivery.identity.contracts.SimulationContext;

/**
 * ✅ Local Redis Geo Repository cho Match Service
 * Lưu bản sao vị trí shipper nhận từ tracking-service qua Kafka
 * Dùng key riêng "match:shippers:geo" để tách biệt với tracking-service
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class MatchRedisGeoRepository {

    private final RedisTemplate<String, Object> redisTemplate;

    private static final String GEO_KEY = "match:shippers:geo";
    private static final String ONLINE_SET_KEY = "match:shippers:online";
    private static final String BUSY_PREFIX = "match:shipper:busy:";
    private static final String OFFER_PREFIX = "match:shipper:offer:";
    private static final String DELIVERY_OFFER_PREFIX = "match:delivery:offer:";
    private static final String DELIVERY_OFFER_SESSION_PREFIX = "match:delivery:offer-session:";
    private static final String CANCELLED_PREFIX = "match:cancelled:";
    private static final String STATUS_VERSION_PREFIX = "match:shipper:status-version:";
    private static final String LOCATION_FRESH_PREFIX = "match:shipper:location-fresh:";
    private static final String COMPLETED_DELIVERIES_PREFIX = "match:shipper:completed-deliveries:";
    private static final String COMPLETION_EVENT_PREFIX = "match:delivery-completion:event:";
    private static final long COMPLETION_EVENT_TTL_SECONDS = 2_592_000L;
    private static final long LOCATION_TTL_SECONDS = 300; // 5 phút — nếu không nhận update thì coi như cũ
    private static final DefaultRedisScript<Long> RESERVE_SHIPPER_OFFER = new DefaultRedisScript<>("""
            if redis.call('EXISTS', KEYS[3]) == 1 then
              return -2
            end
            local currentDelivery = redis.call('GET', KEYS[1])
            if currentDelivery and currentDelivery ~= ARGV[1] then
              return 0
            end
            local currentSession = redis.call('GET', KEYS[4])
            if currentDelivery and (not currentSession or currentSession ~= ARGV[4]) then
              return 0
            end
            local currentShipper = redis.call('GET', KEYS[2])
            if currentShipper and currentShipper ~= ARGV[2] then
              return -1
            end
            redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[3]))
            redis.call('SET', KEYS[2], ARGV[2], 'EX', tonumber(ARGV[3]))
            redis.call('SET', KEYS[4], ARGV[4], 'EX', tonumber(ARGV[3]))
            return 1
            """, Long.class);
    private static final DefaultRedisScript<Long> RELEASE_SHIPPER_OFFER = new DefaultRedisScript<>("""
            local currentDelivery = redis.call('GET', KEYS[1])
            local currentShipper = redis.call('GET', KEYS[2])
            if currentDelivery ~= ARGV[1] or currentShipper ~= ARGV[2] then
              return 0
            end
            local currentSession = redis.call('GET', KEYS[3])
            if ARGV[3] ~= '' and currentSession ~= ARGV[3] then
              return 0
            end
            redis.call('DEL', KEYS[1])
            redis.call('DEL', KEYS[2])
            redis.call('DEL', KEYS[3])
            return 1
            """, Long.class);
    private static final DefaultRedisScript<Long> RESERVE_BATCH_OFFER = new DefaultRedisScript<>("""
            local current = redis.call('GET', KEYS[1])
            if current and current ~= ARGV[1] then return 0 end
            local offset = 2
            local count = tonumber(ARGV[2])
            for i = 1, count do
              local delivery = redis.call('GET', KEYS[offset + i - 1])
              if delivery and delivery ~= ARGV[3] then return 0 end
              if redis.call('EXISTS', KEYS[offset + count + i - 1]) == 1 then return 0 end
            end
            redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[4]))
            for i = 1, count do
              redis.call('SET', KEYS[offset + i - 1], ARGV[3], 'EX', tonumber(ARGV[4]))
              redis.call('SET', KEYS[offset + count + i - 1], ARGV[5], 'EX', tonumber(ARGV[4]))
            end
            return 1
            """, Long.class);
    private static final DefaultRedisScript<Long> RELEASE_BATCH_OFFER = new DefaultRedisScript<>("""
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
            local count = tonumber(ARGV[2])
            for i = 1, count do
              if redis.call('GET', KEYS[1 + i]) ~= ARGV[3] then return 0 end
            end
            redis.call('DEL', KEYS[1])
            for i = 1, count do
              redis.call('DEL', KEYS[1 + i])
              redis.call('DEL', KEYS[1 + count + i])
            end
            return 1
            """, Long.class);
    private static final DefaultRedisScript<Long> APPLY_SHIPPER_STATUS = new DefaultRedisScript<>("""
            local current = redis.call('GET', KEYS[1])
            local incomingTimestamp = tonumber(ARGV[1])
            if current then
              local separator = string.find(current, ':')
              if not separator then
                return -2
              end
              local currentTimestamp = tonumber(string.sub(current, 1, separator - 1))
              local currentEventId = string.sub(current, separator + 1)
              if currentTimestamp > incomingTimestamp then
                return 0
              end
              if currentTimestamp == incomingTimestamp then
                if currentEventId == ARGV[2] then
                  return 0
                end
                return -1
              end
            end

            local currentOffer = redis.call('GET', KEYS[2])
            -- A batch offer is keyed by batch:<uuid>, so it cannot equal the
            -- individual delivery id. AVAILABLE is emitted only after the
            -- batch aggregate completes; clear the shipper-level reservation
            -- unconditionally at that fence.
            if currentOffer and (tonumber(ARGV[4]) == 0 or currentOffer == ARGV[3]) then
              redis.call('DEL', KEYS[2])
            end
            local currentDeliveryOffer = redis.call('GET', KEYS[4])
            if currentDeliveryOffer and currentDeliveryOffer == ARGV[5] then
              redis.call('DEL', KEYS[4])
              redis.call('DEL', KEYS[5])
            end
            if tonumber(ARGV[4]) == 1 then
              redis.call('SET', KEYS[3], 'BUSY', 'EX', 7200)
            else
              redis.call('DEL', KEYS[3])
            end
            redis.call('SET', KEYS[1], tostring(incomingTimestamp) .. ':' .. ARGV[2])
            return 1
            """, Long.class);
    /**
     * The live location projection is shared across Match replicas. A Java
     * get/check/write sequence lets an old online record read before a newer
     * offline tombstone then resurrect the shipper after it commits. Keep the
     * freshness fence and all GEO/online membership mutation inside one Redis
     * script so cross-partition/replay order is monotonic.
     */
    private static final DefaultRedisScript<Long> APPLY_ONLINE_LOCATION = new DefaultRedisScript<>("""
            local current = redis.call('GET', KEYS[1])
            local incomingTimestamp = tonumber(ARGV[1])
            if current then
              local currentTimestamp = tonumber(current)
              if not currentTimestamp then return -2 end
              if currentTimestamp >= incomingTimestamp then return 0 end
            end
            redis.call('GEOADD', KEYS[2], ARGV[2], ARGV[3], ARGV[4])
            redis.call('SADD', KEYS[3], ARGV[4])
            -- Refresh the online membership TTL together with the location
            -- freshness fence. Without this, a set created by an older REAL
            -- projection can expire while a simulation location remains fresh.
            redis.call('EXPIRE', KEYS[3], tonumber(ARGV[5]))
            redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[5]))
            return 1
            """, Long.class);
    private static final DefaultRedisScript<Long> APPLY_OFFLINE_LOCATION = new DefaultRedisScript<>("""
            local current = redis.call('GET', KEYS[1])
            local incomingTimestamp = tonumber(ARGV[1])
            if current then
              local currentTimestamp = tonumber(current)
              if not currentTimestamp then return -2 end
              if currentTimestamp >= incomingTimestamp then return 0 end
            end
            redis.call('SREM', KEYS[3], ARGV[2])
            redis.call('ZREM', KEYS[2], ARGV[2])
            redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[3]))
            return 1
            """, Long.class);
    private static final DefaultRedisScript<Long> RECORD_COMPLETED_DELIVERY = new DefaultRedisScript<>("""
            if redis.call('SET', KEYS[2], '1', 'NX', 'EX', tonumber(ARGV[1])) then
              return redis.call('INCR', KEYS[1])
            end
            return 0
            """, Long.class);

    /**
     * ✅ Thêm/cập nhật vị trí shipper vào Redis Geo local
     */
    public void addOrUpdateShipperLocation(Long shipperId, Double latitude, Double longitude,
                                           Boolean isOnline, long timestamp) {
        addOrUpdateShipperLocation(shipperId, latitude, longitude, isOnline, timestamp, SimulationContext.real());
    }

    public void addOrUpdateShipperLocation(Long shipperId, Double latitude, Double longitude,
                                           Boolean isOnline, long timestamp, SimulationContext context) {
        try {
            if (shipperId == null || shipperId <= 0 || latitude == null || longitude == null
                    || timestamp <= 0) {
                throw new IllegalArgumentException("positive shipperId/timestamp and coordinates are required");
            }

            // Keep this repository API compatible with its earlier contract:
            // callers that pass an explicit offline location must still remove
            // the shipper. The listener currently calls markShipperOffline
            // directly, but other callers must not turn an offline fact into
            // an online GEO entry merely because the shared Redis fence moved
            // into Lua.
            if (!Boolean.TRUE.equals(isOnline)) {
                markShipperOffline(shipperId, timestamp, context);
                return;
            }

            SimulationContext normalized = SimulationContext.orReal(context);
            normalized.requireValid();
            Long result = redisTemplate.execute(APPLY_ONLINE_LOCATION,
                    List.of(scoped(LOCATION_FRESH_PREFIX, normalized) + shipperId,
                            scoped(GEO_KEY, normalized), scoped(ONLINE_SET_KEY, normalized)), timestamp,
                    longitude, latitude, shipperId.toString(), LOCATION_TTL_SECONDS);
            if (result == null || result == -2L) {
                throw new IllegalStateException("Invalid Match location freshness state");
            }
            if (result == 0L) {
                log.debug("Ignoring stale location for shipper {} at {}", shipperId, timestamp);
                return;
            }

            log.debug("📍 [MatchGeo] Updated shipper {} at ({}, {}) online={}", shipperId, latitude, longitude, isOnline);

        } catch (IllegalArgumentException | IllegalStateException exception) {
            throw exception;
        } catch (Exception e) {
            log.error("💥 [MatchGeo] Error updating shipper {}: {}", shipperId, e.getMessage());
            throw new IllegalStateException("Cannot update Match Geo location replica", e);
        }
    }

    /**
     * Remove a shipper from matching using a timestamped tombstone. The timestamp
     * fences a delayed older online update during the location freshness window.
     */
    public void markShipperOffline(Long shipperId, long timestamp) {
        markShipperOffline(shipperId, timestamp, SimulationContext.real());
    }

    public void markShipperOffline(Long shipperId, long timestamp, SimulationContext context) {
        if (shipperId == null || shipperId <= 0 || timestamp <= 0) {
            throw new IllegalArgumentException("positive shipperId and timestamp are required");
        }

        try {
            SimulationContext normalized = SimulationContext.orReal(context);
            normalized.requireValid();
            Long result = redisTemplate.execute(APPLY_OFFLINE_LOCATION,
                    List.of(scoped(LOCATION_FRESH_PREFIX, normalized) + shipperId,
                            scoped(GEO_KEY, normalized), scoped(ONLINE_SET_KEY, normalized)), timestamp,
                    shipperId.toString(), LOCATION_TTL_SECONDS);
            if (result == null || result == -2L) {
                throw new IllegalStateException("Invalid Match location freshness state");
            }
            if (result == 0L) {
                log.debug("Ignoring stale offline tombstone for shipper {} at {}", shipperId, timestamp);
                return;
            }
            log.debug("🔴 [MatchGeo] Marked shipper {} offline at {}", shipperId, timestamp);
        } catch (Exception e) {
            log.error("💥 [MatchGeo] Error marking shipper {} offline: {}", shipperId, e.getMessage());
            throw new IllegalStateException("Cannot apply Match Geo offline tombstone", e);
        }
    }

    /**
     * ✅ Tìm shipper gần trong bán kính — query 100% local, không gọi REST
     */
    public List<NearbyShipperResult> findNearbyShippers(Double lat, Double lng, Double radiusKm, Integer limit) {
        return findNearbyShippers(lat, lng, radiusKm, limit, SimulationContext.real());
    }

    /** Run-scoped GEO lookup; virtual shippers can never enter the real pool. */
    public List<NearbyShipperResult> findNearbyShippers(Double lat, Double lng, Double radiusKm, Integer limit,
                                                        SimulationContext context) {
        SimulationContext normalized = SimulationContext.orReal(context);
        normalized.requireValid();
        String geoKey = scoped(GEO_KEY, normalized);
        String onlineKey = scoped(ONLINE_SET_KEY, normalized);
        String busyPrefix = scoped(BUSY_PREFIX, normalized);
        String offerPrefix = scoped(OFFER_PREFIX, normalized);
        String freshPrefix = scoped(LOCATION_FRESH_PREFIX, normalized);
        List<NearbyShipperResult> results = new ArrayList<>();
        GeoOperations<String, Object> geoOps = redisTemplate.opsForGeo();
        Point center = new Point(lng, lat);
        Distance radius = new Distance(radiusKm, Metrics.KILOMETERS);
        Circle circle = new Circle(center, radius);

        RedisGeoCommands.GeoRadiusCommandArgs args = RedisGeoCommands.GeoRadiusCommandArgs
                .newGeoRadiusArgs()
                .includeDistance()
                .includeCoordinates()
                .sortAscending()
                .limit(limit != null ? limit * 3 : 30); // Lấy nhiều hơn vì sẽ filter busy/offline

        GeoResults<RedisGeoCommands.GeoLocation<Object>> geoResults = geoOps.radius(geoKey, circle, args);

        if (geoResults != null && geoResults.getContent() != null) {
            int count = 0;
            int maxResults = limit != null ? limit : 10;

            for (var geoResult : geoResults.getContent()) {
                if (count >= maxResults) break;

                String shipperIdStr = geoResult.getContent().getName().toString();

                    // Filter: chỉ lấy online + không busy
                boolean isOnline = Boolean.TRUE.equals(
                        redisTemplate.opsForSet().isMember(onlineKey, shipperIdStr));
                boolean isBusy = Boolean.TRUE.equals(redisTemplate.hasKey(busyPrefix + shipperIdStr));
                boolean hasActiveOffer = Boolean.TRUE.equals(redisTemplate.hasKey(offerPrefix + shipperIdStr));
                boolean hasFreshLocation = Boolean.TRUE.equals(redisTemplate.hasKey(freshPrefix + shipperIdStr));

                if (isOnline && hasFreshLocation && !isBusy && !hasActiveOffer) {
                    Long shipperId = Long.parseLong(shipperIdStr);
                    Point coords = geoResult.getContent().getPoint();
                    double distance = geoResult.getDistance().getValue();

                    results.add(new NearbyShipperResult(
                            shipperId, coords.getY(), coords.getX(), distance));
                    count++;
                }
            }
        }

        log.info("🔍 [MatchGeo] Found {} available shippers within {}km from ({}, {})",
                results.size(), radiusKm, lat, lng);

        return results;
    }

    private String scoped(String key, SimulationContext context) {
        return context.isSimulation() ? key + "simulation:" + context.runId() : key;
    }

    /** Idempotently counts canonical delivery completions in the matching namespace. */
    public boolean recordCompletedDelivery(UUID eventId, Long shipperId, SimulationContext context) {
        if (eventId == null || shipperId == null || shipperId <= 0) {
            throw new IllegalArgumentException("delivery completion requires eventId and positive shipperId");
        }
        SimulationContext normalized = SimulationContext.orReal(context);
        normalized.requireValid();
        Long result = redisTemplate.execute(RECORD_COMPLETED_DELIVERY,
                List.of(scoped(COMPLETED_DELIVERIES_PREFIX, normalized) + ":" + shipperId,
                        scoped(COMPLETION_EVENT_PREFIX, normalized) + ":" + eventId), COMPLETION_EVENT_TTL_SECONDS);
        if (result == null) throw new IllegalStateException("Missing completed-delivery projection result");
        return result > 0;
    }

    public long completedDeliveries(Long shipperId, SimulationContext context) {
        if (shipperId == null || shipperId <= 0) return 0L;
        SimulationContext normalized = SimulationContext.orReal(context);
        normalized.requireValid();
        Object value = redisTemplate.opsForValue().get(
                scoped(COMPLETED_DELIVERIES_PREFIX, normalized) + ":" + shipperId);
        if (value == null) return 0L;
        try {
            return Math.max(0L, Long.parseLong(String.valueOf(value)));
        } catch (NumberFormatException invalid) {
            throw new IllegalStateException("Invalid completed-delivery projection state", invalid);
        }
    }

    public boolean tryReserveShipperOffer(
            Long shipperId,
            Long deliveryId,
            UUID matchingSessionId,
            int timeoutSeconds) {
        return tryReserveShipperOffer(shipperId, deliveryId, matchingSessionId, timeoutSeconds,
                SimulationContext.real());
    }

    public boolean tryReserveShipperOffer(Long shipperId, Long deliveryId, UUID matchingSessionId,
                                          int timeoutSeconds, SimulationContext context) {
        if (shipperId == null || shipperId <= 0 || deliveryId == null || deliveryId <= 0
                || matchingSessionId == null) {
            return false;
        }
        int ttlSeconds = Math.max(1, timeoutSeconds);
        try {
            SimulationContext normalized = SimulationContext.orReal(context);
            normalized.requireValid();
            Long result = redisTemplate.execute(
                    RESERVE_SHIPPER_OFFER,
                    List.of(scoped(OFFER_PREFIX, normalized) + shipperId,
                            scoped(DELIVERY_OFFER_PREFIX, normalized) + deliveryId,
                            cancellationKey(deliveryId, matchingSessionId, normalized),
                            scoped(DELIVERY_OFFER_SESSION_PREFIX, normalized) + deliveryId),
                    deliveryId.toString(), shipperId.toString(), ttlSeconds, matchingSessionId.toString());
            if (result == null || result == -1L) {
                throw new IllegalStateException("Contradictory Match offer ownership state");
            }
            return result == 1L;
        } catch (Exception e) {
            log.error("💥 [MatchGeo] Cannot reserve shipper {} for delivery {}: {}",
                    shipperId, deliveryId, e.getMessage());
            throw new IllegalStateException("Cannot reserve shipper offer in Redis", e);
        }
    }

    public boolean releaseShipperOffer(Long shipperId, Long deliveryId, UUID matchingSessionId) {
        return releaseShipperOffer(shipperId, deliveryId, matchingSessionId, SimulationContext.real());
    }

    public boolean releaseShipperOffer(Long shipperId, Long deliveryId, UUID matchingSessionId,
                                       SimulationContext context) {
        if (shipperId == null || shipperId <= 0 || deliveryId == null || deliveryId <= 0) {
            return false;
        }
        try {
            SimulationContext normalized = SimulationContext.orReal(context);
            normalized.requireValid();
            Long result = redisTemplate.execute(
                    RELEASE_SHIPPER_OFFER,
                    List.of(scoped(OFFER_PREFIX, normalized) + shipperId,
                            scoped(DELIVERY_OFFER_PREFIX, normalized) + deliveryId,
                            scoped(DELIVERY_OFFER_SESSION_PREFIX, normalized) + deliveryId),
                    deliveryId.toString(), shipperId.toString(),
                    matchingSessionId == null ? "" : matchingSessionId.toString());
            if (result == null) {
                throw new IllegalStateException("Missing Match offer release result");
            }
            return result == 1L;
        } catch (IllegalStateException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot release Match shipper offer", exception);
        }
    }

    public boolean releaseOfferForDelivery(Long deliveryId, UUID matchingSessionId) {
        if (deliveryId == null || deliveryId <= 0 || matchingSessionId == null) {
            return false;
        }
        try {
            Object owner = redisTemplate.opsForValue().get(DELIVERY_OFFER_PREFIX + deliveryId);
            if (owner == null) {
                return false;
            }
            long shipperId = Long.parseLong(String.valueOf(owner));
            if (shipperId <= 0) {
                throw new IllegalStateException("Invalid Match reverse offer owner");
            }
            return releaseShipperOffer(shipperId, deliveryId, matchingSessionId);
        } catch (IllegalStateException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot release Match offer by delivery", exception);
        }
    }

    /** Atomically reserves one shipper for all deliveries in a proposed batch. */
    public boolean tryReserveShipperBatchOffer(Long shipperId, List<Long> deliveryIds,
                                               UUID batchId, UUID matchingSessionId,
                                               int timeoutSeconds) {
        if (shipperId == null || shipperId <= 0 || batchId == null || matchingSessionId == null
                || deliveryIds == null || deliveryIds.isEmpty() || deliveryIds.size() > 3
                || deliveryIds.stream().anyMatch(id -> id == null || id <= 0)) {
            return false;
        }
        List<String> keys = new ArrayList<>();
        keys.add(OFFER_PREFIX + shipperId);
        deliveryIds.forEach(id -> keys.add(DELIVERY_OFFER_PREFIX + id));
        deliveryIds.forEach(id -> keys.add(DELIVERY_OFFER_SESSION_PREFIX + id));
        Long result = redisTemplate.execute(RESERVE_BATCH_OFFER, keys,
                "batch:" + batchId, deliveryIds.size(), shipperId.toString(),
                Math.max(1, timeoutSeconds), matchingSessionId.toString());
        return result != null && result == 1L;
    }

    /** Releases a batch only when the shipper and every reverse delivery key still agree. */
    public boolean releaseShipperBatchOffer(Long shipperId, List<Long> deliveryIds,
                                            UUID batchId, UUID matchingSessionId) {
        if (shipperId == null || shipperId <= 0 || batchId == null || matchingSessionId == null
                || deliveryIds == null || deliveryIds.isEmpty() || deliveryIds.size() > 3) {
            return false;
        }
        List<String> keys = new ArrayList<>();
        keys.add(OFFER_PREFIX + shipperId);
        deliveryIds.forEach(id -> keys.add(DELIVERY_OFFER_PREFIX + id));
        deliveryIds.forEach(id -> keys.add(DELIVERY_OFFER_SESSION_PREFIX + id));
        Long result = redisTemplate.execute(RELEASE_BATCH_OFFER, keys,
                "batch:" + batchId, deliveryIds.size(), shipperId.toString());
        return result != null && result == 1L;
    }

    public boolean applyShipperStatus(Long shipperId, Long deliveryId, String status,
                                      long timestamp, String eventId) {
        return applyShipperStatus(shipperId, deliveryId, status, timestamp, eventId,
                SimulationContext.real());
    }

    /** Apply a status transition inside the same REAL or run-scoped simulation namespace. */
    public boolean applyShipperStatus(Long shipperId, Long deliveryId, String status,
                                      long timestamp, String eventId, SimulationContext context) {
        if (shipperId == null || shipperId <= 0 || deliveryId == null || deliveryId <= 0
                || timestamp <= 0 || eventId == null || eventId.isBlank()
                || !("BUSY".equals(status) || "AVAILABLE".equals(status))) {
            throw new IllegalArgumentException(
                    "positive identity/timestamp, stable eventId and BUSY/AVAILABLE status are required");
        }
        try {
            SimulationContext normalized = SimulationContext.orReal(context);
            normalized.requireValid();
            Long result = redisTemplate.execute(
                    APPLY_SHIPPER_STATUS,
                    List.of(scoped(STATUS_VERSION_PREFIX, normalized) + shipperId,
                            scoped(OFFER_PREFIX, normalized) + shipperId,
                            scoped(BUSY_PREFIX, normalized) + shipperId,
                            scoped(DELIVERY_OFFER_PREFIX, normalized) + deliveryId,
                            scoped(DELIVERY_OFFER_SESSION_PREFIX, normalized) + deliveryId),
                    timestamp, eventId, deliveryId.toString(),
                    "BUSY".equals(status) ? 1L : 0L, shipperId.toString());
            if (result == null || result == -2L) {
                throw new IllegalStateException("Invalid Match status version state");
            }
            if (result == -1L) {
                throw new IllegalArgumentException(
                        "Conflicting shipper status events share the same timestamp");
            }
            return result == 1L;
        } catch (IllegalArgumentException | IllegalStateException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot apply Match shipper status", exception);
        }
    }

    private String cancellationKey(Long deliveryId, UUID matchingSessionId) {
        return cancellationKey(deliveryId, matchingSessionId, SimulationContext.real());
    }

    private String cancellationKey(Long deliveryId, UUID matchingSessionId, SimulationContext context) {
        return scoped(CANCELLED_PREFIX, SimulationContext.orReal(context)) + deliveryId + ":" + matchingSessionId;
    }

    /**
     * ✅ Data class cho kết quả tìm kiếm
     */
    public static class NearbyShipperResult {
        public final Long shipperId;
        public final Double latitude;
        public final Double longitude;
        public final Double distanceKm;

        public NearbyShipperResult(Long shipperId, Double latitude, Double longitude, Double distanceKm) {
            this.shipperId = shipperId;
            this.latitude = latitude;
            this.longitude = longitude;
            this.distanceKm = distanceKm;
        }
    }
}
