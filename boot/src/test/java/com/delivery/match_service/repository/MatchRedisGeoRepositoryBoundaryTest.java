package com.delivery.match_service.repository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
class MatchRedisGeoRepositoryBoundaryTest {
    private final RedisTemplate<String, Object> redis = mock(RedisTemplate.class);
    private final ValueOperations<String, Object> values = mock(ValueOperations.class);
    private final UUID session = UUID.randomUUID();
    private MatchRedisGeoRepository repository;

    @BeforeEach
    void setUp() {
        repository = new MatchRedisGeoRepository(redis);
    }

    @Test
    void invalidOfferIdentitiesNeverTouchRedis() {
        for (Long invalid : Arrays.asList(null, 0L, -1L)) {
            assertThat(repository.tryReserveShipperOffer(invalid, 11L, session, 1)).isFalse();
            assertThat(repository.tryReserveShipperOffer(7L, invalid, session, 1)).isFalse();
            assertThat(repository.releaseShipperOffer(invalid, 11L, session)).isFalse();
            assertThat(repository.releaseShipperOffer(7L, invalid, session)).isFalse();
            assertThat(repository.releaseOfferForDelivery(invalid, session)).isFalse();
            assertThat(repository.tryReserveShipperBatchOffer(invalid, List.of(11L), session, session, 1)).isFalse();
            assertThat(repository.releaseShipperBatchOffer(invalid, List.of(11L), session, session)).isFalse();
        }
        assertThat(repository.tryReserveShipperOffer(7L, 11L, null, 1)).isFalse();
        assertThat(repository.releaseOfferForDelivery(11L, null)).isFalse();
        for (List<Long> invalid : Arrays.<List<Long>>asList(null, List.of(), List.of(1L, 2L, 3L, 4L))) {
            assertThat(repository.tryReserveShipperBatchOffer(7L, invalid, session, session, 1)).isFalse();
            assertThat(repository.releaseShipperBatchOffer(7L, invalid, session, session)).isFalse();
        }
        for (List<Long> invalid : List.of(Arrays.asList((Long) null), List.of(0L), List.of(-1L))) {
            assertThat(repository.tryReserveShipperBatchOffer(7L, invalid, session, session, 1)).isFalse();
        }
        assertThat(repository.tryReserveShipperBatchOffer(7L, List.of(11L), null, session, 1)).isFalse();
        assertThat(repository.tryReserveShipperBatchOffer(7L, List.of(11L), session, null, 1)).isFalse();
        assertThat(repository.releaseShipperBatchOffer(7L, List.of(11L), null, session)).isFalse();
        assertThat(repository.releaseShipperBatchOffer(7L, List.of(11L), session, null)).isFalse();
        verifyNoInteractions(redis);
    }

    @Test
    void locationAndStatusValidateEveryRequiredInput() {
        for (Long invalid : Arrays.asList(null, 0L, -1L)) {
            assertThatThrownBy(() -> repository.addOrUpdateShipperLocation(invalid, 10.0, 106.0, true, 1))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("coordinates are required");
            assertThatThrownBy(() -> repository.markShipperOffline(invalid, 1))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("timestamp are required");
            assertInvalidStatus(invalid, 11L, "BUSY", 1, "event");
            assertInvalidStatus(7L, invalid, "BUSY", 1, "event");
            assertThatThrownBy(() -> repository.recordCompletedDelivery(session, invalid, null))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("positive shipperId");
            assertThat(repository.completedDeliveries(invalid, null)).isZero();
        }
        assertThatThrownBy(() -> repository.addOrUpdateShipperLocation(7L, null, 106.0, true, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> repository.addOrUpdateShipperLocation(7L, 10.0, null, true, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> repository.addOrUpdateShipperLocation(7L, 10.0, 106.0, true, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> repository.markShipperOffline(7L, 0)).isInstanceOf(IllegalArgumentException.class);
        assertInvalidStatus(7L, 11L, "BUSY", 0, "event");
        for (String invalid : Arrays.asList(null, "", " ")) assertInvalidStatus(7L, 11L, "BUSY", 1, invalid);
        for (String invalid : Arrays.asList(null, "busy", "OFFLINE")) assertInvalidStatus(7L, 11L, invalid, 1, "event");
        assertThatThrownBy(() -> repository.recordCompletedDelivery(null, 7L, null))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(redis);
    }

    @Test
    void completionProjectionHandlesAbsentNegativeMalformedAndDuplicateState() {
        when(redis.opsForValue()).thenReturn(values);
        when(values.get("match:shipper:completed-deliveries::7")).thenReturn(null, "-3", "12", "invalid");
        assertThat(repository.completedDeliveries(7L, null)).isZero();
        assertThat(repository.completedDeliveries(7L, null)).isZero();
        assertThat(repository.completedDeliveries(7L, null)).isEqualTo(12);
        assertThatThrownBy(() -> repository.completedDeliveries(7L, null))
                .isInstanceOf(IllegalStateException.class).hasMessage("Invalid completed-delivery projection state");
        when(redis.execute(any(DefaultRedisScript.class), anyList(), eq(2_592_000L))).thenReturn(1L, 0L, null);
        assertThat(repository.recordCompletedDelivery(session, 7L, null)).isTrue();
        assertThat(repository.recordCompletedDelivery(session, 7L, null)).isFalse();
        assertThatThrownBy(() -> repository.recordCompletedDelivery(session, 7L, null))
                .isInstanceOf(IllegalStateException.class).hasMessage("Missing completed-delivery projection result");
    }

    @Test
    void reverseOfferLookupRejectsMissingOrCorruptOwnership() {
        when(redis.opsForValue()).thenReturn(values);
        when(values.get("match:delivery:offer:11")).thenReturn(null, "0", "not-a-shipper");
        assertThat(repository.releaseOfferForDelivery(11L, session)).isFalse();
        assertThatThrownBy(() -> repository.releaseOfferForDelivery(11L, session))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Invalid");
        assertThatThrownBy(() -> repository.releaseOfferForDelivery(11L, session))
                .isInstanceOf(IllegalStateException.class).hasMessage("Cannot release Match offer by delivery")
                .hasCauseInstanceOf(NumberFormatException.class);
    }

    @Test
    void missingAndCorruptScriptResultsFailClosed() {
        when(redis.execute(any(DefaultRedisScript.class), anyList(), any(Object[].class))).thenReturn(null, -2L);
        for (int attempt = 0; attempt < 2; attempt++) {
            assertThatThrownBy(() -> repository.addOrUpdateShipperLocation(7L, 10.0, 106.0, true, 1))
                    .isInstanceOf(IllegalStateException.class).hasMessage("Invalid Match location freshness state");
        }
        when(redis.execute(any(DefaultRedisScript.class), anyList(), any(Object[].class))).thenReturn(null, -2L);
        for (int attempt = 0; attempt < 2; attempt++) {
            assertThatThrownBy(() -> repository.markShipperOffline(7L, 1))
                    .isInstanceOf(IllegalStateException.class).hasMessage("Cannot apply Match Geo offline tombstone")
                    .hasRootCauseMessage("Invalid Match location freshness state");
        }
        when(redis.execute(any(DefaultRedisScript.class), anyList(), any(Object[].class))).thenReturn(null, -2L);
        for (int attempt = 0; attempt < 2; attempt++) {
            assertThatThrownBy(() -> repository.applyShipperStatus(7L, 11L, "BUSY", 1, "event"))
                    .isInstanceOf(IllegalStateException.class).hasMessage("Invalid Match status version state");
        }
        when(redis.execute(any(DefaultRedisScript.class), anyList(), any(Object[].class))).thenReturn(null, -1L);
        for (int attempt = 0; attempt < 2; attempt++) {
            assertThatThrownBy(() -> repository.tryReserveShipperOffer(7L, 11L, session, 1))
                    .isInstanceOf(IllegalStateException.class).hasMessage("Cannot reserve shipper offer in Redis")
                    .hasRootCauseMessage("Contradictory Match offer ownership state");
        }
        when(redis.execute(any(DefaultRedisScript.class), anyList(), any(Object[].class))).thenReturn(null);
        assertThatThrownBy(() -> repository.releaseShipperOffer(7L, 11L, null))
                .isInstanceOf(IllegalStateException.class).hasMessage("Missing Match offer release result");
        assertThat(repository.tryReserveShipperBatchOffer(7L, List.of(11L), session, session, 0)).isFalse();
        assertThat(repository.releaseShipperBatchOffer(7L, List.of(11L), session, session)).isFalse();
        when(redis.execute(any(DefaultRedisScript.class), anyList(), any(Object[].class))).thenReturn(0L);
        assertThat(repository.tryReserveShipperBatchOffer(7L, List.of(11L), session, session, 0)).isFalse();
        assertThat(repository.releaseShipperBatchOffer(7L, List.of(11L), session, session)).isFalse();
    }

    @Test
    void infrastructureExceptionsKeepTheirCause() {
        RuntimeException failure = new RuntimeException("redis unavailable");
        when(redis.execute(any(DefaultRedisScript.class), anyList(), any(Object[].class))).thenThrow(failure);
        assertThatThrownBy(() -> repository.addOrUpdateShipperLocation(7L, 10.0, 106.0, true, 1))
                .isInstanceOf(IllegalStateException.class).hasMessage("Cannot update Match Geo location replica").hasCause(failure);
        assertThatThrownBy(() -> repository.markShipperOffline(7L, 1))
                .isInstanceOf(IllegalStateException.class).hasCause(failure);
        assertThatThrownBy(() -> repository.applyShipperStatus(7L, 11L, "AVAILABLE", 1, "event"))
                .isInstanceOf(IllegalStateException.class).hasMessage("Cannot apply Match shipper status").hasCause(failure);
        assertThatThrownBy(() -> repository.releaseShipperOffer(7L, 11L, session))
                .isInstanceOf(IllegalStateException.class).hasCause(failure);
    }

    private void assertInvalidStatus(Long shipper, Long delivery, String status, long timestamp, String event) {
        assertThatThrownBy(() -> repository.applyShipperStatus(shipper, delivery, status, timestamp, event))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("BUSY/AVAILABLE status are required");
    }
}
