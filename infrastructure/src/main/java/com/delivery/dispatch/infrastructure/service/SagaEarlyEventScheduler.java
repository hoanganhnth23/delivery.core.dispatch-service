package com.delivery.dispatch.infrastructure.service;

import com.delivery.dispatch.infrastructure.repository.SagaEarlyEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Recovers the narrow race where an early event commits immediately after
 * order.created has checked its staging table. Processing is idempotent at the
 * Saga inbox and aggregate lock, so multiple replicas may observe the same ID
 * without creating a second side effect.
 */
@Slf4j
@Component
public class SagaEarlyEventScheduler {
    private final SagaEarlyEventRepository repository;
    private final DispatchCoordinator dispatchCoordinator;
    private final int batchSize;

    public SagaEarlyEventScheduler(
            SagaEarlyEventRepository repository,
            DispatchCoordinator dispatchCoordinator,
            @Value("${app.saga.early-event-batch-size:100}") int batchSize) {
        this.repository = repository;
        this.dispatchCoordinator = dispatchCoordinator;
        this.batchSize = Math.max(1, Math.min(batchSize, 500));
    }

    @Scheduled(fixedDelayString = "${app.saga.early-event-poll-delay-ms:5000}")
    public void processReadyEvents() {
        for (UUID eventId : repository.findReadyEventIds(PageRequest.of(0, batchSize))) {
            try {
                dispatchCoordinator.processEarlyEvent(eventId);
            } catch (Exception failure) {
                log.error("Failed to apply staged Saga eventId={}", eventId, failure);
            }
        }
    }
}
