package com.delivery.saga_orchestrator_service.repository;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.delivery.saga_orchestrator_service.entity.SagaInboundReceipt;

public interface SagaInboundReceiptRepository extends JpaRepository<SagaInboundReceipt, UUID> {

    /**
     * PostgreSQL waits for an in-flight primary-key conflict, then returns zero
     * after the first transaction commits. This lets duplicate Kafka deliveries
     * on separate replicas converge to a replay check rather than a
     * rollback-only unique-key failure.
     */
    @Modifying(flushAutomatically = true)
    @Query(value = """
            INSERT INTO saga_inbound_receipts (
                event_id, topic, order_id, payload_fingerprint, received_at
            ) VALUES (
                :eventId, :topic, :orderId, :payloadFingerprint, CURRENT_TIMESTAMP
            ) ON CONFLICT (event_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsentPostgres(
            @Param("eventId") UUID eventId,
            @Param("topic") String topic,
            @Param("orderId") Long orderId,
            @Param("payloadFingerprint") String payloadFingerprint);

    /** H2 fallback for focused unit/integration tests; production uses ON CONFLICT. */
    @Modifying(flushAutomatically = true)
    @Query(value = """
            INSERT INTO saga_inbound_receipts (
                event_id, topic, order_id, payload_fingerprint, received_at
            ) SELECT
                :eventId, :topic, :orderId, :payloadFingerprint, CURRENT_TIMESTAMP
            WHERE NOT EXISTS (
                SELECT 1 FROM saga_inbound_receipts WHERE event_id = :eventId
            )
            """, nativeQuery = true)
    int insertIfAbsentH2(
            @Param("eventId") UUID eventId,
            @Param("topic") String topic,
            @Param("orderId") Long orderId,
            @Param("payloadFingerprint") String payloadFingerprint);
}
