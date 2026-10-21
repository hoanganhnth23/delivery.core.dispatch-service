package com.delivery.match_service.service.impl;

import com.delivery.match_service.repository.MatchRedisGeoRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MatchCancellationServiceImplTest {

    private static final UUID MATCHING_SESSION =
            UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");

    @Mock RedisTemplate<String, Object> redisTemplate;
    @Mock ValueOperations<String, Object> values;
    @Mock MatchRedisGeoRepository repository;

    private MatchCancellationServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new MatchCancellationServiceImpl(redisTemplate, repository);
        when(redisTemplate.opsForValue()).thenReturn(values);
    }

    @Test
    void persistsTombstoneBeforeReleasingCurrentOffer() {
        service.markCancelled(11L, MATCHING_SESSION);

        var order = inOrder(values, repository);
        order.verify(values).set("match:cancelled:11:" + MATCHING_SESSION,
                Boolean.TRUE, Duration.ofHours(2));
        order.verify(repository).releaseOfferForDelivery(11L, MATCHING_SESSION);
    }

    @Test
    void offerReleaseFailurePropagatesForKafkaRetry() {
        doThrow(new IllegalStateException("redis unavailable"))
                .when(repository).releaseOfferForDelivery(11L, MATCHING_SESSION);

        assertThatThrownBy(() -> service.markCancelled(11L, MATCHING_SESSION))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot persist matching cancellation");
    }
}
