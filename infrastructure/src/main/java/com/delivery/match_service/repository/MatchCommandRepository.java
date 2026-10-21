package com.delivery.match_service.repository;

import com.delivery.match_service.entity.MatchCommand;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import jakarta.persistence.LockModeType;
import java.util.Optional;
import java.util.UUID;

public interface MatchCommandRepository extends JpaRepository<MatchCommand, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select command from MatchCommand command where command.eventId = :eventId")
    Optional<MatchCommand> findByEventIdForUpdate(@Param("eventId") UUID eventId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            select command
            from MatchCommand command
            where command.deliveryId = :deliveryId
              and command.matchingSessionId = :matchingSessionId
            """)
    Optional<MatchCommand> findByDeliveryAndSessionForUpdate(
            @Param("deliveryId") Long deliveryId,
            @Param("matchingSessionId") UUID matchingSessionId);
}
