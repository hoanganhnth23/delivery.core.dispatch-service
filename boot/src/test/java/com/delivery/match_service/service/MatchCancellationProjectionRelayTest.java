package com.delivery.match_service.service;

import com.delivery.match_service.entity.MatchCancellationTombstone;
import com.delivery.match_service.repository.MatchCancellationTombstoneRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MatchCancellationProjectionRelayTest {

    private static final UUID STOP_EVENT_ID =
            UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID SESSION_ID =
            UUID.fromString("22222222-2222-2222-2222-222222222222");

    @Mock
    private MatchCancellationTombstoneRepository repository;

    @Mock
    private MatchCancellationService cancellationService;

    private MatchCancellationProjectionRelay relay;

    @BeforeEach
    void setUp() {
        relay = new MatchCancellationProjectionRelay(repository, cancellationService);
        ReflectionTestUtils.setField(relay, "batchSize", 10);
    }

    @Test
    void marksDurableTombstoneProjectedAfterRedisCancellationSucceeds() {
        MatchCancellationTombstone tombstone = pendingTombstone();
        when(repository.findByDeliveryAndSessionForUpdate(123L, SESSION_ID))
                .thenReturn(Optional.of(tombstone));

        assertThat(relay.projectNow(123L, SESSION_ID)).isTrue();

        assertThat(tombstone.getProjectionStatus())
                .isEqualTo(MatchCancellationTombstone.ProjectionStatus.PROJECTED);
        assertThat(tombstone.getRedisProjectedAt()).isNotNull();
        assertThat(tombstone.getLastProjectionError()).isNull();
        verify(cancellationService).markCancelled(123L, SESSION_ID);
        verify(repository).save(tombstone);
    }

    @Test
    void retainsProjectionForDurableRetryWhenRedisIsUnavailable() {
        MatchCancellationTombstone tombstone = pendingTombstone();
        when(repository.findByDeliveryAndSessionForUpdate(123L, SESSION_ID))
                .thenReturn(Optional.of(tombstone));
        doThrow(new IllegalStateException("redis unavailable"))
                .when(cancellationService).markCancelled(123L, SESSION_ID);

        assertThat(relay.projectNow(123L, SESSION_ID)).isFalse();

        assertThat(tombstone.getProjectionStatus())
                .isEqualTo(MatchCancellationTombstone.ProjectionStatus.PENDING);
        assertThat(tombstone.getProjectionAttempts()).isEqualTo(1);
        assertThat(tombstone.getNextProjectionAttemptAt()).isAfter(tombstone.getCreatedAt());
        assertThat(tombstone.getLastProjectionError()).contains("redis unavailable");
        verify(repository).save(tombstone);
    }

    @Test
    void scheduledRelayClaimsAndProjectsThePendingTombstone() {
        MatchCancellationTombstone tombstone = pendingTombstone();
        when(repository.lockNextPendingProjectionBatch(10)).thenReturn(List.of(tombstone));

        relay.relayPending();

        assertThat(tombstone.getProjectionStatus())
                .isEqualTo(MatchCancellationTombstone.ProjectionStatus.PROJECTED);
        verify(cancellationService).markCancelled(123L, SESSION_ID);
        verify(repository).save(tombstone);
    }

    private MatchCancellationTombstone pendingTombstone() {
        return new MatchCancellationTombstone(
                STOP_EVENT_ID, 456L, 123L, SESSION_ID, "a".repeat(64));
    }
}
