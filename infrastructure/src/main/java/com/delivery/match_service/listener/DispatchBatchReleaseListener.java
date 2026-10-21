package com.delivery.match_service.listener;

import com.delivery.match.domain.batch.BatchPoolPolicy;

import com.delivery.match_service.entity.DispatchPoolItem;
import com.delivery.match_service.repository.DispatchPoolItemRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import lombok.RequiredArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

/** Retires the old Match generation after Delivery retires or completes a batch. */
@Component
@RequiredArgsConstructor
public class DispatchBatchReleaseListener {
    private final DispatchPoolItemRepository poolRepository;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = {
            "${app.kafka.topics.batch-released:delivery.batch.released}",
            "${app.kafka.topics.batch-completed:delivery.batch.completed}"
    })
    @Transactional
    public void handle(String message, Acknowledgment acknowledgment) {
        try {
            JsonNode root = objectMapper.readTree(message);
            JsonNode deliveryIds = root.path("deliveryIds");
            JsonNode sessions = root.path("matchingSessionIds");
            if (!deliveryIds.isArray() || !sessions.isArray() || deliveryIds.size() != sessions.size()) {
                throw new IllegalArgumentException("Batch release delivery/session identity is invalid");
            }
            for (int i = 0; i < deliveryIds.size(); i++) {
                Long deliveryId = deliveryIds.get(i).asLong();
                String sessionText = sessions.get(i).asText();
                if (sessionText == null || sessionText.isBlank()) continue;
                UUID sessionId = UUID.fromString(sessionText);
                DispatchPoolItem item = poolRepository.findByDeliveryAndSessionForUpdate(deliveryId, sessionId)
                        .orElse(null);
                if (item == null || !BatchPoolPolicy.retiredByBatchRelease(
                        BatchPoolPolicy.State.valueOf(item.getState().name()))) continue;
                // Saga receives the same release fact and emits a new matching
                // generation. Retire this old generation so it cannot race
                // with the newly enqueued pool item.
                item.setState(DispatchPoolItem.State.EXPIRED);
                item.setClaimedRoundId(null);
                item.setUpdatedAt(LocalDateTime.now());
                poolRepository.save(item);
            }
            acknowledgeAfterCommit(acknowledgment);
        } catch (IllegalArgumentException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalStateException("Cannot requeue released dispatch batch", ex);
        }
    }

    /** A rolled-back retirement must leave the release offset uncommitted for redelivery. */
    private static void acknowledgeAfterCommit(Acknowledgment acknowledgment) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            acknowledgment.acknowledge();
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void afterCommit() { acknowledgment.acknowledge(); }
        });
    }
}
