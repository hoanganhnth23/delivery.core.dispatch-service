package com.delivery.saga_orchestrator_service.repository;

import com.delivery.saga_orchestrator_service.entity.SagaOutboxEvent;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

public interface SagaOutboxEventRepository extends JpaRepository<SagaOutboxEvent, Long> {
    Optional<SagaOutboxEvent> findFirstByAggregateIdAndTopicOrderByIdDesc(String aggregateId, String topic);

    /**
     * Claim at most the earliest unsent command for each order. An earlier retry
     * (or DEAD command awaiting operator action) blocks later commands for that
     * order, preserving per-order Kafka ordering across relay instances.
     */
    @Query(value = """
            SELECT event.*
            FROM saga_outbox_events event
            WHERE event.status = 'PENDING'
              AND event.next_attempt_at <= CURRENT_TIMESTAMP
              AND NOT EXISTS (
                  SELECT 1
                  FROM saga_outbox_events predecessor
                  WHERE predecessor.aggregate_id = event.aggregate_id
                    AND predecessor.status IN ('PENDING', 'DEAD')
                    AND (predecessor.created_at < event.created_at
                      OR (predecessor.created_at = event.created_at AND predecessor.id < event.id))
              )
            ORDER BY event.created_at, event.id
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<SagaOutboxEvent> lockNextOrderedBatch(@Param("batchSize") int batchSize);

    @Query(value = """
            SELECT event.*
            FROM saga_outbox_events event
            WHERE (
                    (event.status = 'PENDING' AND event.next_attempt_at <= CURRENT_TIMESTAMP)
                    OR (event.status = 'IN_FLIGHT'
                        AND (event.lease_until IS NULL OR event.lease_until <= CURRENT_TIMESTAMP))
                  )
              AND NOT EXISTS (
                  SELECT 1
                  FROM saga_outbox_events predecessor
                  WHERE predecessor.aggregate_id = event.aggregate_id
                    AND predecessor.status IN ('PENDING', 'IN_FLIGHT', 'DEAD')
                    AND (predecessor.created_at < event.created_at
                      OR (predecessor.created_at = event.created_at AND predecessor.id < event.id))
              )
            ORDER BY event.created_at, event.id
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<SagaOutboxEvent> lockNextClaimableBatch(@Param("batchSize") int batchSize);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select event from SagaOutboxEvent event where event.id = :id")
    Optional<SagaOutboxEvent> findByIdForUpdate(@Param("id") Long id);
}
