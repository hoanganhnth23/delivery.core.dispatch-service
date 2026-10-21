package com.delivery.match_service.service;

import com.delivery.match_service.entity.MatchCancellationTombstone;
import com.delivery.match_service.repository.MatchCancellationTombstoneRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Rebuilds Redis's cancellation projection from Match's durable generation
 * tombstone. A Redis outage must never turn a successfully persisted stop into
 * a permanently lost offer release after Kafka's finite retry budget expires.
 */
@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(name = "app.cancellation-projection.relay-enabled", havingValue = "true")
public class MatchCancellationProjectionRelay {

    private final MatchCancellationTombstoneRepository repository;
    private final MatchCancellationService cancellationService;

    @Value("${app.cancellation-projection.batch-size:50}")
    private int batchSize;

    /**
     * Try immediately for normal latency. A false result is still durable and
     * will be retried by the scheduler; callers must ACK the Kafka stop after
     * the PostgreSQL tombstone has committed.
     */
    @Transactional
    public boolean projectNow(Long deliveryId, UUID matchingSessionId) {
        MatchCancellationTombstone tombstone = repository
                .findByDeliveryAndSessionForUpdate(deliveryId, matchingSessionId)
                .orElseThrow(() -> new IllegalStateException(
                        "Cannot project a missing Match cancellation tombstone"));
        return project(tombstone);
    }

    @Scheduled(fixedDelayString = "${app.cancellation-projection.poll-delay-ms:1000}")
    @Transactional
    public void relayPending() {
        for (MatchCancellationTombstone tombstone : repository.lockNextPendingProjectionBatch(
                Math.max(1, Math.min(batchSize, 500)))) {
            project(tombstone);
        }
    }

    private boolean project(MatchCancellationTombstone tombstone) {
        if (tombstone.getProjectionStatus()
                == MatchCancellationTombstone.ProjectionStatus.PROJECTED) {
            return true;
        }
        try {
            cancellationService.markCancelled(
                    tombstone.getDeliveryId(), tombstone.getMatchingSessionId());
            tombstone.setProjectionStatus(MatchCancellationTombstone.ProjectionStatus.PROJECTED);
            tombstone.setRedisProjectedAt(LocalDateTime.now());
            tombstone.setLastProjectionError(null);
            repository.save(tombstone);
            return true;
        } catch (Exception failure) {
            int attempts = tombstone.getProjectionAttempts() + 1;
            tombstone.setProjectionAttempts(attempts);
            long delaySeconds = Math.min(300L,
                    1L << Math.min(Math.max(attempts - 1, 0), 8));
            tombstone.setNextProjectionAttemptAt(LocalDateTime.now().plusSeconds(delaySeconds));
            tombstone.setLastProjectionError(abbreviate(failure.getMessage()));
            repository.save(tombstone);
            log.warn("Match cancellation projection for delivery {} generation {} retry {} in {}s",
                    tombstone.getDeliveryId(), tombstone.getMatchingSessionId(), attempts, delaySeconds,
                    failure);
            return false;
        }
    }

    private String abbreviate(String message) {
        if (message == null) {
            return "Unknown Redis cancellation projection failure";
        }
        return message.length() <= 2000 ? message : message.substring(0, 2000);
    }
}
