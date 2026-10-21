package com.delivery.match_service.repository;

import com.delivery.identity.contracts.SimulationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Proves a newer offline tombstone always wins a concurrent old online update in Redis. */
@SpringJUnitConfig(classes = MatchRedisLocationConcurrencyIntegrationTest.RedisConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class MatchRedisLocationConcurrencyIntegrationTest {

    private static final long SHIPPER_ID = 91L;

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @org.springframework.beans.factory.annotation.Autowired private MatchRedisGeoRepository repository;
    @org.springframework.beans.factory.annotation.Autowired private RedisTemplate<String, Object> redis;

    @BeforeEach
    void clearRedis() {
        redis.getConnectionFactory().getConnection().serverCommands().flushDb();
    }

    @Test
    void concurrentOlderOnlineAndNewerOfflineConvergeToOffline() throws Exception {
        long timestamp = System.currentTimeMillis();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Throwable> online = executor.submit(() -> together(ready, start, () ->
                    repository.addOrUpdateShipperLocation(SHIPPER_ID, 10.77, 106.70, true, timestamp)));
            Future<Throwable> offline = executor.submit(() -> together(ready, start, () ->
                    repository.markShipperOffline(SHIPPER_ID, timestamp + 1)));
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            assertThat(online.get(20, TimeUnit.SECONDS)).isNull();
            assertThat(offline.get(20, TimeUnit.SECONDS)).isNull();
        } finally {
            executor.shutdownNow();
        }

        assertThat(redis.opsForSet().isMember("match:shippers:online", Long.toString(SHIPPER_ID))).isFalse();
        assertThat(redis.opsForGeo().position("match:shippers:geo", Long.toString(SHIPPER_ID)))
                .containsOnlyNulls();
    }

    @Test
    void legacyOfflineLocationCallStillProjectsAnOfflineTombstone() {
        long timestamp = System.currentTimeMillis();
        repository.addOrUpdateShipperLocation(SHIPPER_ID, 10.77, 106.70, true, timestamp);

        repository.addOrUpdateShipperLocation(SHIPPER_ID, 10.77, 106.70, false, timestamp + 1);

        assertThat(redis.opsForSet().isMember("match:shippers:online", Long.toString(SHIPPER_ID))).isFalse();
        assertThat(redis.opsForGeo().position("match:shippers:geo", Long.toString(SHIPPER_ID)))
                .containsOnlyNulls();
    }

    @Test
    void onlineProjectionRefreshesSharedOnlineSetTtl() {
        redis.opsForSet().add("match:shippers:online", Long.toString(SHIPPER_ID));
        redis.expire("match:shippers:online", java.time.Duration.ofSeconds(1));
        long timestamp = System.currentTimeMillis();

        repository.addOrUpdateShipperLocation(SHIPPER_ID, 10.77, 106.70, true, timestamp);

        assertThat(redis.getExpire("match:shippers:online")).isGreaterThan(250L);
    }

    @Test
    void completionProjectionIsIdempotentAndNeverMixesSimulationWithRealCounters() {
        UUID eventId = UUID.randomUUID();
        UUID runId = UUID.randomUUID();
        SimulationContext simulation = new SimulationContext(SimulationContext.ExecutionMode.SIMULATION,
                runId, UUID.randomUUID(), 1L);

        assertThat(repository.recordCompletedDelivery(eventId, SHIPPER_ID, simulation)).isTrue();
        assertThat(repository.recordCompletedDelivery(eventId, SHIPPER_ID, simulation)).isFalse();
        assertThat(repository.completedDeliveries(SHIPPER_ID, simulation)).isEqualTo(1L);
        assertThat(redis.hasKey("match:shipper:completed-deliveries:simulation:" + runId + ":" + SHIPPER_ID))
                .isTrue();
        assertThat(repository.completedDeliveries(SHIPPER_ID, SimulationContext.real())).isZero();

        assertThat(repository.recordCompletedDelivery(UUID.randomUUID(), SHIPPER_ID, SimulationContext.real())).isTrue();
        assertThat(repository.completedDeliveries(SHIPPER_ID, SimulationContext.real())).isEqualTo(1L);
        assertThat(repository.completedDeliveries(SHIPPER_ID, simulation)).isEqualTo(1L);
    }

    private Throwable together(CountDownLatch ready, CountDownLatch start, Operation operation) {
        try {
            ready.countDown();
            if (!start.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Location race did not start");
            operation.run();
            return null;
        } catch (Throwable failure) {
            return failure;
        }
    }

    @FunctionalInterface
    private interface Operation { void run() throws Exception; }

    @TestConfiguration
    static class RedisConfiguration {
        @Bean
        LettuceConnectionFactory redisConnectionFactory() {
            return new LettuceConnectionFactory(new RedisStandaloneConfiguration(
                    REDIS.getHost(), REDIS.getMappedPort(6379)));
        }

        @Bean
        RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
            RedisTemplate<String, Object> template = new RedisTemplate<>();
            template.setConnectionFactory(connectionFactory);
            template.setKeySerializer(new StringRedisSerializer());
            template.setHashKeySerializer(new StringRedisSerializer());
            GenericJackson2JsonRedisSerializer valueSerializer = new GenericJackson2JsonRedisSerializer();
            template.setValueSerializer(valueSerializer);
            template.setHashValueSerializer(valueSerializer);
            template.afterPropertiesSet();
            return template;
        }

        @Bean
        MatchRedisGeoRepository matchRedisGeoRepository(RedisTemplate<String, Object> redis) {
            return new MatchRedisGeoRepository(redis);
        }
    }
}
