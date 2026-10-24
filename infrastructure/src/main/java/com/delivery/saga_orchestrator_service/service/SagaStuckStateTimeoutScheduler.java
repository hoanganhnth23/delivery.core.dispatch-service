package com.delivery.saga_orchestrator_service.service;

import com.delivery.saga_orchestrator_service.entity.SagaInstance.SagaStatus;
import com.delivery.saga_orchestrator_service.repository.SagaInstanceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class SagaStuckStateTimeoutScheduler {
    private final SagaInstanceRepository cases;
    private final SagaStuckStateTimeoutService service;
    @Value("${app.saga.timeout-batch-size:100}") private int batchSize = 100;
    @Value("${app.saga.timeout.offer-persisting-seconds:120}") private long persistingSeconds = 120;
    @Value("${app.saga.timeout.compensating-seconds:120}") private long compensatingSeconds = 120;
    @Value("${app.saga.timeout.offer-retiring-seconds:120}") private long retiringSeconds = 120;
    @Value("${app.saga.timeout.max-resends:3}") private int maxResends = 3;
    @Value("${app.kafka.topics.cache-shipper-found:saga.command.cache-shipper-found}")
    private String persistTopic = SagaManager.CMD_CACHE_SHIPPER_FOUND;
    @Value("${app.kafka.topics.cancel-delivery:saga.command.cancel-delivery}")
    private String cancelTopic = SagaManager.CMD_CANCEL_DELIVERY;
    @Value("${app.kafka.topics.expire-shipper-offer:saga.command.expire-shipper-offer}")
    private String retireTopic = SagaManager.CMD_EXPIRE_SHIPPER_OFFER;

    @Scheduled(fixedDelayString = "${app.saga.timeout-poll-delay-ms:30000}")
    public void checkTimeouts() {
        check(SagaStatus.OFFER_PERSISTING, persistingSeconds, persistTopic);
        check(SagaStatus.COMPENSATING, compensatingSeconds, cancelTopic);
        check(SagaStatus.OFFER_RETIRING, retiringSeconds, retireTopic);
    }

    private void check(SagaStatus state, long seconds, String topic) {
        Duration timeout = Duration.ofSeconds(Math.max(1, seconds));
        for (var saga : cases.findStuckReplyCases(state, LocalDateTime.now().minus(timeout),
                PageRequest.of(0, Math.max(1, Math.min(batchSize, 500))))) {
            try {
                service.expire(saga.getOrderId(), state, timeout, Math.max(0, maxResends), topic);
            } catch (Exception failure) {
                log.error("Failed stuck-state timeout caseId={}, orderId={}, state={}",
                        saga.getId(), saga.getOrderId(), state, failure);
            }
        }
    }
}
