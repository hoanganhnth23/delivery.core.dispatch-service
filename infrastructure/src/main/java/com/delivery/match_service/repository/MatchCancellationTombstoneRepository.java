package com.delivery.match_service.repository;

import com.delivery.match_service.entity.MatchCancellationTombstone;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;
import java.util.List;

public interface MatchCancellationTombstoneRepository
        extends JpaRepository<MatchCancellationTombstone, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            select tombstone
            from MatchCancellationTombstone tombstone
            where tombstone.deliveryId = :deliveryId
              and tombstone.matchingSessionId = :matchingSessionId
            """)
    Optional<MatchCancellationTombstone> findByDeliveryAndSessionForUpdate(
            @Param("deliveryId") Long deliveryId,
            @Param("matchingSessionId") UUID matchingSessionId);

    /**
     * Each Match replica can safely rebuild the volatile Redis cancellation
     * projection after an outage without sharing an in-memory queue.
     */
    @Query(value = """
            SELECT *
            FROM match_cancellation_tombstones
            WHERE projection_status = 'PENDING'
              AND next_projection_attempt_at <= CURRENT_TIMESTAMP
            ORDER BY next_projection_attempt_at, created_at, event_id
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<MatchCancellationTombstone> lockNextPendingProjectionBatch(
            @Param("batchSize") int batchSize);

    boolean existsByDeliveryIdAndMatchingSessionId(Long deliveryId, UUID matchingSessionId);
}
