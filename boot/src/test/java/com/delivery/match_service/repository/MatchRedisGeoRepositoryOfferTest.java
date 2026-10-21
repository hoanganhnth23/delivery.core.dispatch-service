package com.delivery.match_service.repository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MatchRedisGeoRepositoryOfferTest {

    private static final UUID MATCHING_SESSION =
            UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");

    @Mock RedisTemplate<String, Object> redisTemplate;
    @Mock ValueOperations<String, Object> values;
    @Mock SetOperations<String, Object> sets;

    private MatchRedisGeoRepository repository;

    @BeforeEach
    void setUp() {
        repository = new MatchRedisGeoRepository(redisTemplate);
        lenient().when(redisTemplate.opsForValue()).thenReturn(values);
    }

    @Test
    @SuppressWarnings("unchecked")
    void reservesOfferWithAtomicForwardAndReverseOwnership() {
        when(redisTemplate.execute(
                any(DefaultRedisScript.class),
                eq(List.of("match:shipper:offer:7", "match:delivery:offer:11",
                        "match:cancelled:11:" + MATCHING_SESSION,
                        "match:delivery:offer-session:11")),
                eq("11"), eq("7"), eq(180), eq(MATCHING_SESSION.toString())))
                .thenReturn(1L);

        assertThat(repository.tryReserveShipperOffer(7L, 11L, MATCHING_SESSION, 180)).isTrue();
    }

    @Test
    @SuppressWarnings("unchecked")
    void rejectsReservationOwnedByAnotherDelivery() {
        when(redisTemplate.execute(
                any(DefaultRedisScript.class), anyList(),
                eq("11"), eq("7"), eq(180), eq(MATCHING_SESSION.toString())))
                .thenReturn(0L);

        assertThat(repository.tryReserveShipperOffer(7L, 11L, MATCHING_SESSION, 180)).isFalse();
    }

    @Test
    @SuppressWarnings("unchecked")
    void cancelledDeliveryCannotAcquireOfferAfterTombstone() {
        when(redisTemplate.execute(
                any(DefaultRedisScript.class), anyList(),
                eq("11"), eq("7"), eq(180), eq(MATCHING_SESSION.toString())))
                .thenReturn(-2L);

        assertThat(repository.tryReserveShipperOffer(7L, 11L, MATCHING_SESSION, 180)).isFalse();
    }

    @Test
    @SuppressWarnings("unchecked")
    void propagatesRedisFailureInsteadOfReportingReservationRace() {
        when(redisTemplate.execute(
                any(DefaultRedisScript.class), anyList(),
                eq("11"), eq("7"), eq(180), eq(MATCHING_SESSION.toString())))
                .thenThrow(new RuntimeException("redis unavailable"));

        assertThatThrownBy(() -> repository.tryReserveShipperOffer(7L, 11L, MATCHING_SESSION, 180))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot reserve shipper offer");
    }

    @Test
    @SuppressWarnings("unchecked")
    void releasesOnlyMatchingForwardAndReverseOwnership() {
        when(values.get("match:delivery:offer:11")).thenReturn("7");
        when(redisTemplate.execute(
                any(DefaultRedisScript.class),
                eq(List.of("match:shipper:offer:7", "match:delivery:offer:11",
                        "match:delivery:offer-session:11")),
                eq("11"), eq("7"), eq(MATCHING_SESSION.toString())))
                .thenReturn(1L);

        assertThat(repository.releaseOfferForDelivery(11L, MATCHING_SESSION)).isTrue();
    }

    @Test
    @SuppressWarnings("unchecked")
    void staleReleaseCannotDeleteDifferentOfferGeneration() {
        when(redisTemplate.execute(
                any(DefaultRedisScript.class),
                eq(List.of("match:shipper:offer:7", "match:delivery:offer:11",
                        "match:delivery:offer-session:11")),
                eq("11"), eq("7"), eq(MATCHING_SESSION.toString())))
                .thenReturn(0L);

        assertThat(repository.releaseShipperOffer(7L, 11L, MATCHING_SESSION)).isFalse();
    }

    @Test
    @SuppressWarnings("unchecked")
    void offlineTombstoneRemovesMatchingMembershipAndFencesOlderUpdates() {
        when(redisTemplate.execute(
                any(DefaultRedisScript.class),
                eq(List.of("match:shipper:location-fresh:7", "match:shippers:geo", "match:shippers:online")),
                eq(200L), eq("7"), eq(300L))).thenReturn(1L);

        repository.markShipperOffline(7L, 200L);

        verify(redisTemplate).execute(
                any(DefaultRedisScript.class),
                eq(List.of("match:shipper:location-fresh:7", "match:shippers:geo", "match:shippers:online")),
                eq(200L), eq("7"), eq(300L));
    }

    @Test
    @SuppressWarnings("unchecked")
    void staleOfflineTombstoneCannotRemoveNewerOnlineState() {
        when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), eq(200L), eq("7"), eq(300L)))
                .thenReturn(0L);

        repository.markShipperOffline(7L, 200L);

        verify(redisTemplate).execute(any(DefaultRedisScript.class), anyList(), eq(200L), eq("7"), eq(300L));
    }

    @Test
    @SuppressWarnings("unchecked")
    void legacyOfflineLocationCallUsesTheAtomicOfflineTombstone() {
        when(redisTemplate.execute(
                any(DefaultRedisScript.class),
                eq(List.of("match:shipper:location-fresh:7", "match:shippers:geo", "match:shippers:online")),
                eq(200L), eq("7"), eq(300L))).thenReturn(1L);

        repository.addOrUpdateShipperLocation(7L, 10.77, 106.70, false, 200L);

        verify(redisTemplate).execute(
                any(DefaultRedisScript.class),
                eq(List.of("match:shipper:location-fresh:7", "match:shippers:geo", "match:shippers:online")),
                eq(200L), eq("7"), eq(300L));
        verify(redisTemplate, never()).opsForGeo();
        verify(redisTemplate, never()).opsForSet();
    }

    @Test
    @SuppressWarnings("unchecked")
    void statusMutationUsesAtomicVersionOfferAndBusyScript() {
        String eventId = "11111111-1111-1111-1111-111111111111";
        when(redisTemplate.execute(
                any(DefaultRedisScript.class),
                eq(List.of("match:shipper:status-version:7",
                        "match:shipper:offer:7", "match:shipper:busy:7",
                        "match:delivery:offer:11", "match:delivery:offer-session:11")),
                eq(200L), eq(eventId), eq("11"), eq(1L), eq("7")))
                .thenReturn(1L);

        assertThat(repository.applyShipperStatus(7L, 11L, "BUSY", 200L, eventId)).isTrue();
    }

    @Test
    @SuppressWarnings("unchecked")
    void exactOrStaleStatusReplayIsAnIdempotentNoOp() {
        String eventId = "11111111-1111-1111-1111-111111111111";
        when(redisTemplate.execute(
                any(DefaultRedisScript.class), anyList(),
                eq(200L), eq(eventId), eq("11"), eq(0L), eq("7")))
                .thenReturn(0L);

        assertThat(repository.applyShipperStatus(7L, 11L, "AVAILABLE", 200L, eventId)).isFalse();
    }

    @Test
    @SuppressWarnings("unchecked")
    void sameTimestampWithDifferentEventIdFailsClosed() {
        String eventId = "22222222-2222-2222-2222-222222222222";
        when(redisTemplate.execute(
                any(DefaultRedisScript.class), anyList(),
                eq(200L), eq(eventId), eq("11"), eq(1L), eq("7")))
                .thenReturn(-1L);

        assertThatThrownBy(() -> repository.applyShipperStatus(7L, 11L, "BUSY", 200L, eventId))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("same timestamp");
    }
}
