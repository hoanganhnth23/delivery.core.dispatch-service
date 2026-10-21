package com.delivery.match_service.repository;

import com.delivery.match_service.entity.DispatchPoolItem;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import jakarta.persistence.LockModeType;
import java.util.Optional;
import java.util.List;
import java.util.UUID;
import java.time.LocalDateTime;
import org.springframework.data.domain.Pageable;

public interface DispatchPoolItemRepository extends JpaRepository<DispatchPoolItem, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            select item
            from DispatchPoolItem item
            where item.deliveryId = :deliveryId
              and item.matchingSessionId = :matchingSessionId
            """)
    Optional<DispatchPoolItem> findByDeliveryAndSessionForUpdate(
            @Param("deliveryId") Long deliveryId,
            @Param("matchingSessionId") UUID matchingSessionId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            select item
            from DispatchPoolItem item
            where item.state = com.delivery.match_service.entity.DispatchPoolItem$State.WAITING
              and (item.pickupH3Cell = :zone or (:zone = 'LEGACY' and item.pickupH3Cell is null))
              and item.eligibleAt <= :now
              and item.matchingDeadlineAt > :now
            order by item.matchingDeadlineAt asc, item.eligibleAt asc, item.poolItemId asc
            """)
    List<DispatchPoolItem> findReadyByZoneForUpdate(
            @Param("zone") String zone,
            @Param("now") LocalDateTime now,
            Pageable pageable);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            select item
            from DispatchPoolItem item
            where item.state = com.delivery.match_service.entity.DispatchPoolItem$State.WAITING
              and item.pickupH3Cell in :zones
              and item.eligibleAt <= :now
              and item.matchingDeadlineAt > :now
            order by item.matchingDeadlineAt asc, item.eligibleAt asc, item.poolItemId asc
            """)
    List<DispatchPoolItem> findReadyByZonesForUpdate(
            @Param("zones") List<String> zones,
            @Param("now") LocalDateTime now,
            Pageable pageable);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            select item
            from DispatchPoolItem item
            where item.state = com.delivery.match_service.entity.DispatchPoolItem$State.WAITING
              and item.matchingDeadlineAt <= :now
            order by item.matchingDeadlineAt asc, item.updatedAt asc, item.poolItemId asc
            """)
    List<DispatchPoolItem> findExpiredWaitingForUpdate(
            @Param("now") LocalDateTime now,
            Pageable pageable);

    @Query("""
            select distinct coalesce(item.pickupH3Cell, 'LEGACY')
            from DispatchPoolItem item
            where item.state = com.delivery.match_service.entity.DispatchPoolItem$State.WAITING
              and item.eligibleAt <= :now
              and item.matchingDeadlineAt > :now
            """)
    List<String> findReadyZones(@Param("now") LocalDateTime now, Pageable pageable);

    @Query("select item from DispatchPoolItem item where item.claimedRoundId = :roundId order by item.matchingDeadlineAt asc, item.poolItemId asc")
    List<DispatchPoolItem> findByClaimedRoundId(@Param("roundId") UUID roundId);

    /** Locks the claimed items so a concurrent stop-matching waits for the round decision. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select item from DispatchPoolItem item where item.claimedRoundId = :roundId order by item.matchingDeadlineAt asc, item.poolItemId asc")
    List<DispatchPoolItem> findByClaimedRoundIdForUpdate(@Param("roundId") UUID roundId);
}
