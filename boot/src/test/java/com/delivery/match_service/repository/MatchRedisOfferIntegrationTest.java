package com.delivery.match_service.repository;

import com.delivery.match_service.service.MatchCancellationService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.kafka.listener.auto-startup=false",
        "spring.data.redis.host=127.0.0.1",
        "spring.data.redis.port=6379"
})
@EnabledIfEnvironmentVariable(named = "MATCH_REDIS_INTEGRATION", matches = "true")
class MatchRedisOfferIntegrationTest {

    private static final long DELIVERY_ONE = 910001L;
    private static final long DELIVERY_TWO = 910002L;
    private static final long SHIPPER = 920001L;
    private static final long LOCATION_SHIPPER = 920002L;
    private static final UUID SESSION_ONE = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static final UUID SESSION_TWO = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

    @Autowired MatchRedisGeoRepository repository;
    @Autowired MatchCancellationService cancellationService;
    @Autowired RedisTemplate<String, Object> redisTemplate;

    @BeforeEach
    @AfterEach
    void cleanFixtureKeys() {
        redisTemplate.delete("match:shipper:offer:" + SHIPPER);
        redisTemplate.delete("match:delivery:offer:" + DELIVERY_ONE);
        redisTemplate.delete("match:delivery:offer:" + DELIVERY_TWO);
        redisTemplate.delete("match:delivery:offer-session:" + DELIVERY_ONE);
        redisTemplate.delete("match:delivery:offer-session:" + DELIVERY_TWO);
        redisTemplate.delete("match:cancelled:" + DELIVERY_ONE + ":" + SESSION_ONE);
        redisTemplate.delete("match:cancelled:" + DELIVERY_ONE + ":" + SESSION_TWO);
        redisTemplate.delete("match:cancelled:" + DELIVERY_TWO + ":" + SESSION_ONE);
        redisTemplate.delete("match:cancelled:" + DELIVERY_TWO + ":" + SESSION_TWO);
        redisTemplate.opsForGeo().remove("match:shippers:geo", String.valueOf(LOCATION_SHIPPER));
        redisTemplate.opsForSet().remove("match:shippers:online", String.valueOf(LOCATION_SHIPPER));
        redisTemplate.delete("match:shipper:location-fresh:" + LOCATION_SHIPPER);
    }

    @Test
    void cancellationReleasesMatchingOfferAndFencesFutureReservation() {
        assertThat(repository.tryReserveShipperOffer(SHIPPER, DELIVERY_ONE, SESSION_ONE, 180)).isTrue();
        assertThat(redisTemplate.opsForValue().get("match:shipper:offer:" + SHIPPER))
                .isEqualTo(String.valueOf(DELIVERY_ONE));
        assertThat(redisTemplate.opsForValue().get("match:delivery:offer:" + DELIVERY_ONE))
                .isEqualTo(String.valueOf(SHIPPER));

        cancellationService.markCancelled(DELIVERY_ONE, SESSION_ONE);

        assertThat(redisTemplate.hasKey("match:shipper:offer:" + SHIPPER)).isFalse();
        assertThat(redisTemplate.hasKey("match:delivery:offer:" + DELIVERY_ONE)).isFalse();
        assertThat(repository.tryReserveShipperOffer(SHIPPER, DELIVERY_ONE, SESSION_ONE, 180)).isFalse();
    }

    @Test
    void staleReleaseCannotDeleteANewerDeliveryOffer() {
        assertThat(repository.tryReserveShipperOffer(SHIPPER, DELIVERY_TWO, SESSION_TWO, 180)).isTrue();

        assertThat(repository.releaseShipperOffer(SHIPPER, DELIVERY_ONE, SESSION_ONE)).isFalse();

        assertThat(redisTemplate.opsForValue().get("match:shipper:offer:" + SHIPPER))
                .isEqualTo(String.valueOf(DELIVERY_TWO));
        assertThat(redisTemplate.opsForValue().get("match:delivery:offer:" + DELIVERY_TWO))
                .isEqualTo(String.valueOf(SHIPPER));
    }

    @Test
    void staleGenerationCancellationCannotReleaseANewerRematchOffer() {
        assertThat(repository.tryReserveShipperOffer(SHIPPER, DELIVERY_ONE, SESSION_TWO, 180)).isTrue();

        cancellationService.markCancelled(DELIVERY_ONE, SESSION_ONE);

        assertThat(redisTemplate.opsForValue().get("match:shipper:offer:" + SHIPPER))
                .isEqualTo(String.valueOf(DELIVERY_ONE));
        assertThat(redisTemplate.opsForValue().get("match:delivery:offer-session:" + DELIVERY_ONE))
                .isEqualTo(SESSION_TWO.toString());
    }

    @Test
    void offlineTombstoneFencesOlderOnlineReplayInRealRedis() {
        long onlineTimestamp = System.currentTimeMillis();
        repository.addOrUpdateShipperLocation(LOCATION_SHIPPER, 10.77, 106.70, true, onlineTimestamp);
        assertThat(nearbyShipperIds()).contains(LOCATION_SHIPPER);

        long offlineTimestamp = onlineTimestamp + 100;
        repository.markShipperOffline(LOCATION_SHIPPER, offlineTimestamp);
        assertThat(nearbyShipperIds()).doesNotContain(LOCATION_SHIPPER);

        repository.addOrUpdateShipperLocation(
                LOCATION_SHIPPER, 10.77, 106.70, true, onlineTimestamp + 50);

        assertThat(nearbyShipperIds()).doesNotContain(LOCATION_SHIPPER);
    }

    @Test
    void newerOnlineUpdateAfterOfflineTombstoneRestoresMatchingEligibility() {
        long timestamp = System.currentTimeMillis();
        repository.markShipperOffline(LOCATION_SHIPPER, timestamp);

        repository.addOrUpdateShipperLocation(
                LOCATION_SHIPPER, 10.77, 106.70, true, timestamp + 100);

        assertThat(nearbyShipperIds()).contains(LOCATION_SHIPPER);
    }

    @Test
    void concurrentCrossReplicaOnlineAndNewerOfflineConvergeToOfflineTombstone() throws Exception {
        long onlineTimestamp = System.currentTimeMillis();
        java.util.concurrent.CountDownLatch ready = new java.util.concurrent.CountDownLatch(2);
        java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            java.util.concurrent.Future<Throwable> online = executor.submit(() -> together(ready, start, () ->
                    repository.addOrUpdateShipperLocation(
                            LOCATION_SHIPPER, 10.77, 106.70, true, onlineTimestamp)));
            java.util.concurrent.Future<Throwable> offline = executor.submit(() -> together(ready, start, () ->
                    repository.markShipperOffline(LOCATION_SHIPPER, onlineTimestamp + 1)));
            assertThat(ready.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            start.countDown();
            assertThat(online.get(20, java.util.concurrent.TimeUnit.SECONDS)).isNull();
            assertThat(offline.get(20, java.util.concurrent.TimeUnit.SECONDS)).isNull();
        } finally {
            executor.shutdownNow();
        }

        assertThat(nearbyShipperIds()).doesNotContain(LOCATION_SHIPPER);
    }

    private Throwable together(java.util.concurrent.CountDownLatch ready,
                               java.util.concurrent.CountDownLatch start,
                               Operation operation) {
        try {
            ready.countDown();
            if (!start.await(10, java.util.concurrent.TimeUnit.SECONDS)) {
                throw new IllegalStateException("Concurrent Match location test did not start");
            }
            operation.run();
            return null;
        } catch (Throwable failure) {
            return failure;
        }
    }

    @FunctionalInterface
    private interface Operation {
        void run() throws Exception;
    }

    private java.util.List<Long> nearbyShipperIds() {
        return repository.findNearbyShippers(10.77, 106.70, 5.0, 10)
                .stream()
                .map(result -> result.shipperId)
                .toList();
    }
}
