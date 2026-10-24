package com.delivery.saga_orchestrator_service.repository;

import com.delivery.saga_orchestrator_service.entity.SagaInstance;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.data.domain.Pageable;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import jakarta.persistence.LockModeType;

@Repository
public interface SagaInstanceRepository extends JpaRepository<SagaInstance, UUID> {

    @Query("SELECT s FROM SagaInstance s WHERE s.status = :status AND "
            + "COALESCE(s.stuckLastResentAt, s.stateEnteredAt) <= :cutoff "
            + "ORDER BY s.stateEnteredAt ASC, s.id ASC")
    List<SagaInstance> findStuckReplyCases(@Param("status") SagaInstance.SagaStatus status,
            @Param("cutoff") LocalDateTime cutoff, Pageable pageable);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT s FROM SagaInstance s WHERE s.orderId = :orderId")
    Optional<SagaInstance> findByOrderIdForUpdate(@Param("orderId") Long orderId);

    /**
     * ✅ TỐI ƯU: Tìm các saga bị kẹt trực tiếp bằng SQL
     * Chỉ lấy các đơn có status phù hợp và updatedAt cũ hơn thời gian cutoff
     */
    @Query("SELECT s FROM SagaInstance s WHERE s.status = :status AND s.updatedAt < :cutoff "
            + "ORDER BY s.updatedAt ASC, s.id ASC")
    List<SagaInstance> findStuckSagas(
            @Param("status") SagaInstance.SagaStatus status,
            @Param("cutoff") LocalDateTime cutoff,
            Pageable pageable);
}
