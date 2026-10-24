package com.delivery.saga_orchestrator_service.repository;

import com.delivery.saga_orchestrator_service.entity.SagaEarlyEvent;
import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface SagaEarlyEventRepository extends JpaRepository<SagaEarlyEvent, UUID> {

    /** Atomically stages one pre-aggregate cross-topic fact per event identity. */
    @Modifying(flushAutomatically = true)
    @Query(value = """
            INSERT INTO saga_early_events (
                event_id, topic, order_id, payload, payload_fingerprint, received_at
            ) VALUES (
                :eventId, :topic, :orderId, :payload, :payloadFingerprint, CURRENT_TIMESTAMP
            ) ON CONFLICT (event_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsentPostgres(
            @Param("eventId") UUID eventId,
            @Param("topic") String topic,
            @Param("orderId") Long orderId,
            @Param("payload") String payload,
            @Param("payloadFingerprint") String payloadFingerprint);

    /** H2 fallback for focused tests; PostgreSQL is the replica-safe path. */
    @Modifying(flushAutomatically = true)
    @Query(value = """
            INSERT INTO saga_early_events (
                event_id, topic, order_id, payload, payload_fingerprint, received_at
            ) SELECT
                :eventId, :topic, :orderId, :payload, :payloadFingerprint, CURRENT_TIMESTAMP
            WHERE NOT EXISTS (
                SELECT 1 FROM saga_early_events WHERE event_id = :eventId
            )
            """, nativeQuery = true)
    int insertIfAbsentH2(
            @Param("eventId") UUID eventId,
            @Param("topic") String topic,
            @Param("orderId") Long orderId,
            @Param("payload") String payload,
            @Param("payloadFingerprint") String payloadFingerprint);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT e FROM SagaEarlyEvent e WHERE e.orderId = :orderId ORDER BY e.receivedAt ASC, e.eventId ASC")
    List<SagaEarlyEvent> findByOrderIdForUpdate(@Param("orderId") Long orderId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT e FROM SagaEarlyEvent e WHERE e.eventId = :eventId")
    Optional<SagaEarlyEvent> findByIdForUpdate(@Param("eventId") UUID eventId);

    @Query("SELECT e.eventId FROM SagaEarlyEvent e WHERE EXISTS "
            + "(SELECT s FROM SagaInstance s WHERE s.orderId = e.orderId) "
            + "ORDER BY e.receivedAt ASC, e.eventId ASC")
    List<UUID> findReadyEventIds(Pageable pageable);
}
