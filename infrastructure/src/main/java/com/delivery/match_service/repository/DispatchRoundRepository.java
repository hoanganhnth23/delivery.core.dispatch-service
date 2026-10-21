package com.delivery.match_service.repository;

import com.delivery.match_service.entity.DispatchRound;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.data.domain.Pageable;
import jakarta.persistence.LockModeType;

import java.util.UUID;
import java.util.Optional;
import java.util.List;
import java.time.LocalDateTime;

public interface DispatchRoundRepository extends JpaRepository<DispatchRound, UUID> {

    Optional<DispatchRound> findFirstByH3ZoneAndStateOrderByOpenedAtAsc(
            String h3Zone, DispatchRound.State state);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select round from DispatchRound round where round.state = com.delivery.match_service.entity.DispatchRound$State.OPEN and round.cutoffAt <= :now order by round.cutoffAt asc")
    List<DispatchRound> findOpenDueForUpdate(@Param("now") LocalDateTime now, Pageable pageable);
}
