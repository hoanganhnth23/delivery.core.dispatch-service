package com.delivery.dispatch.infrastructure.service;

import com.delivery.dispatch.infrastructure.entity.SagaEarlyEvent;
import com.delivery.dispatch.infrastructure.entity.SagaInboundReceipt;
import com.delivery.dispatch.infrastructure.repository.SagaEarlyEventRepository;
import com.delivery.dispatch.infrastructure.repository.SagaInboundReceiptRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Inbound idempotency for the dispatch case.
 *
 * <ul>
 *   <li>{@link #claim}: every Kafka event is claimed by eventId before any case
 *       mutation or command write; an exact replay is skipped, a contradictory
 *       replay fails.</li>
 *   <li>{@link #stage}: a fact that arrives before its case exists is stored
 *       durably and applied when the case is created.</li>
 * </ul>
 */
@Slf4j
final class DispatchInbox {

    private final SagaInboundReceiptRepository inboundReceiptRepository;
    private final SagaEarlyEventRepository earlyEventRepository;
    private final ObjectMapper objectMapper;
    private final Supplier<String> dataSourceUrl;

    DispatchInbox(SagaInboundReceiptRepository inboundReceiptRepository,
                  SagaEarlyEventRepository earlyEventRepository,
                  ObjectMapper objectMapper, Supplier<String> dataSourceUrl) {
        this.inboundReceiptRepository = inboundReceiptRepository;
        this.earlyEventRepository = earlyEventRepository;
        this.objectMapper = objectMapper;
        this.dataSourceUrl = dataSourceUrl;
    }

    /**
     * Claims a Kafka event before any Saga mutation or command-outbox write. The
     * primary-key claim commits with the Saga mutation/outbox so a concurrent
     * duplicate cannot perform a side effect and a conflicting replay is never
     * silently acknowledged.
     *
     * @return true when this delivery of the event must be applied
     */
    boolean claim(String topic, Long orderId, String rawEvent) {
        if (inboundReceiptRepository == null) return true;
        try {
            JsonNode event = objectMapper.readTree(rawEvent);
            JsonNode id = event.get("eventId");
            if (id == null || !id.isTextual()) {
                throw new IllegalArgumentException("Saga inbound eventId is required");
            }
            UUID eventId = UUID.fromString(id.asText());
            String fingerprint = sha256(rawEvent);
            var existing = inboundReceiptRepository.findById(eventId).orElse(null);
            if (existing == null) {
                if (insertInboundReceiptIfAbsent(eventId, topic, orderId, fingerprint) == 1) {
                    return true;
                }
                existing = inboundReceiptRepository.findById(eventId).orElseThrow(() ->
                        new IllegalStateException("Saga inbound receipt conflict resolved without a committed row"));
            }
            requireExactInboundReplay(existing, topic, orderId, fingerprint);
            log.info("[Saga] Exact inbound replay eventId={}, topic={}, orderId={}, skipping",
                    eventId, topic, orderId);
            return false;
        } catch (IllegalArgumentException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to claim Saga inbound event", exception);
        }
    }

    boolean stagingAvailable() {
        return earlyEventRepository != null;
    }

    /** Staged facts for this order, locked; the caller holds the case lock first. */
    List<SagaEarlyEvent> lockStaged(Long orderId) {
        return earlyEventRepository.findByOrderIdForUpdate(orderId);
    }

    SagaEarlyEvent find(UUID eventId) {
        return earlyEventRepository.findById(eventId).orElse(null);
    }

    SagaEarlyEvent lock(UUID eventId) {
        return earlyEventRepository.findByIdForUpdate(eventId).orElse(null);
    }

    void discard(SagaEarlyEvent staged) {
        earlyEventRepository.delete(staged);
    }

    void stage(String topic, Long orderId, String rawEvent) {
        if (earlyEventRepository == null) {
            throw new IllegalStateException("Early Saga event staging is unavailable for orderId=" + orderId);
        }
        try {
            JsonNode event = objectMapper.readTree(rawEvent);
            JsonNode id = event.get("eventId");
            if (id == null || !id.isTextual()) {
                throw new IllegalArgumentException("Saga early eventId is required");
            }
            UUID eventId = UUID.fromString(id.asText());
            String fingerprint = sha256(rawEvent);
            SagaEarlyEvent existing = earlyEventRepository.findById(eventId).orElse(null);
            if (existing == null) {
                if (insertEarlyEventIfAbsent(eventId, topic, orderId, rawEvent, fingerprint) == 1) {
                    log.info("[Saga] Staged {} before Saga creation for orderId={}, eventId={}",
                            topic, orderId, eventId);
                    return;
                }
                existing = earlyEventRepository.findById(eventId).orElseThrow(() ->
                        new IllegalStateException("Saga early-event conflict resolved without a committed row"));
            }
            requireExactEarlyReplay(existing, topic, orderId, fingerprint);
            log.info("[Saga] Exact early {} replay staged for orderId={}, eventId={}",
                    topic, orderId, eventId);
        } catch (IllegalArgumentException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to stage early Saga event", exception);
        }
    }

    private boolean usesH2() {
        String url = dataSourceUrl.get();
        return url != null && url.startsWith("jdbc:h2:");
    }

    private int insertInboundReceiptIfAbsent(UUID eventId, String topic, Long orderId, String fingerprint) {
        if (usesH2()) {
            return inboundReceiptRepository.insertIfAbsentH2(eventId, topic, orderId, fingerprint);
        }
        return inboundReceiptRepository.insertIfAbsentPostgres(eventId, topic, orderId, fingerprint);
    }

    private int insertEarlyEventIfAbsent(UUID eventId, String topic, Long orderId,
                                         String payload, String fingerprint) {
        if (usesH2()) {
            return earlyEventRepository.insertIfAbsentH2(eventId, topic, orderId, payload, fingerprint);
        }
        return earlyEventRepository.insertIfAbsentPostgres(eventId, topic, orderId, payload, fingerprint);
    }

    private static void requireExactInboundReplay(SagaInboundReceipt existing, String topic, Long orderId,
                                                  String fingerprint) {
        if (!existing.getTopic().equals(topic) || !existing.getOrderId().equals(orderId)
                || !existing.getPayloadFingerprint().equals(fingerprint)) {
            throw new IllegalArgumentException("Saga eventId replay has a contradictory payload");
        }
    }

    private static void requireExactEarlyReplay(SagaEarlyEvent existing, String topic, Long orderId,
                                                String fingerprint) {
        if (!existing.getTopic().equals(topic) || !existing.getOrderId().equals(orderId)
                || !existing.getPayloadFingerprint().equals(fingerprint)) {
            throw new IllegalArgumentException("Saga early eventId replay has a contradictory payload");
        }
    }

    private static String sha256(String payload) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(payload.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(64);
            for (byte value : digest) hex.append(String.format("%02x", value));
            return hex.toString();
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }
}
