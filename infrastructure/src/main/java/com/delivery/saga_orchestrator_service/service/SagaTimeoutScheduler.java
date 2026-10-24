package com.delivery.saga_orchestrator_service.service;

import com.delivery.saga_orchestrator_service.entity.SagaInstance;
import com.delivery.saga_orchestrator_service.entity.SagaInstance.SagaStatus;
import com.delivery.saga_orchestrator_service.repository.SagaInstanceRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

/**
 * ✅ Saga Timeout Scheduler — Phát hiện saga bị "treo" và tự động compensation
 *
 * Kịch bản:
 * - Saga ở trạng thái STARTED nhưng không có delivery.created.result sau 2 phút
 * → TIMEOUT
 * - Saga ở trạng thái FINDING_SHIPPER quá 5 phút → TIMEOUT
 * - Saga ở trạng thái SHIPPER_FOUND được kiểm theo deadline trong
 *   chính payload offer (foundAt + waitingTimeoutSeconds)
 */
@Slf4j
@Component
public class SagaTimeoutScheduler {

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaManager sagaManager;
    private final int batchSize;

    // Timeout thresholds (phút)
    private static final int STARTED_TIMEOUT_MINUTES = 2;
    // ✅ Nhà hàng cần thời gian xác nhận đơn → cho ngưỡng dài hơn.
    private static final int RESTAURANT_CONFIRM_TIMEOUT_MINUTES = 10;
    private static final int FINDING_SHIPPER_TIMEOUT_MINUTES = 5;
    private static final int MIN_SHIPPER_OFFER_TIMEOUT_SECONDS = 1;

    public SagaTimeoutScheduler(SagaInstanceRepository sagaInstanceRepository,
            SagaManager sagaManager,
            @Value("${app.saga.timeout-batch-size:100}") int batchSize) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaManager = sagaManager;
        this.batchSize = Math.max(1, Math.min(batchSize, 500));
    }

    /**
     * Chạy mỗi 30 giây — kiểm tra saga bị timeout
     */
    @Scheduled(fixedDelayString = "${app.saga.timeout-poll-delay-ms:30000}")
    public void checkTimeouts() {
        checkStuckSagas(SagaStatus.STARTED, STARTED_TIMEOUT_MINUTES, "Delivery creation timeout");
        // DELIVERY_CREATED = đang chờ nhà hàng confirm (sau khi bật gate confirm).
        checkStuckSagas(SagaStatus.DELIVERY_CREATED, RESTAURANT_CONFIRM_TIMEOUT_MINUTES, "Restaurant confirmation timeout");
        checkStuckSagas(SagaStatus.FINDING_SHIPPER, FINDING_SHIPPER_TIMEOUT_MINUTES, "Shipper search timeout");
        checkShipperOfferTimeouts();
    }

    private void checkShipperOfferTimeouts() {
        // waitingTimeoutSeconds is part of the offer contract and may be shorter
        // than the default 180 seconds. Query from the minimum supported age;
        // SagaManager locks the aggregate and checks the exact offer deadline
        // before it mutates state, so an unexpired candidate remains a no-op.
        LocalDateTime cutoff = LocalDateTime.now().minusSeconds(MIN_SHIPPER_OFFER_TIMEOUT_SECONDS);
        List<SagaInstance> candidates = sagaInstanceRepository.findStuckSagas(
                SagaStatus.SHIPPER_FOUND, cutoff, PageRequest.of(0, batchSize));

        for (SagaInstance saga : candidates) {
            try {
                sagaManager.handleTimeout(SagaTimeoutCommand.forShipperOffer(
                        saga, "Shipper offer timeout"));
            } catch (Exception failure) {
                log.error("Failed to process shipper-offer timeout for orderId={}",
                        saga.getOrderId(), failure);
            }
        }
    }

    private void checkStuckSagas(SagaStatus status, int timeoutMinutes, String reason) {
        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(timeoutMinutes);
        List<SagaInstance> stuckSagas = sagaInstanceRepository.findStuckSagas(
                status, cutoff, PageRequest.of(0, batchSize));

        for (SagaInstance saga : stuckSagas) {
            try {
                log.warn("⏰ [Saga] TIMEOUT — orderId={}, status={}, stuck since {}, reason={}",
                        saga.getOrderId(), status, saga.getUpdatedAt(), reason);

                // A scheduler observation is not a transition authority. The
                // manager re-locks and verifies this exact status/version before
                // it creates compensation side effects.
                SagaTimeoutCommand timeout = SagaTimeoutCommand.forStage(
                        saga, status, Duration.ofMinutes(timeoutMinutes), reason);
                sagaManager.handleTimeout(timeout);
            } catch (Exception failure) {
                // Leave this Saga eligible for the next poll, but do not let one
                // poison aggregate starve every later timeout in the batch.
                log.error("Failed to process timeout for orderId={}, status={}",
                        saga.getOrderId(), status, failure);
            }
        }
    }
}
