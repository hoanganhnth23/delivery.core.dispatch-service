package com.delivery.saga_orchestrator_service.service;

import com.delivery.dispatch.application.DefaultStuckStateTimeoutUseCase;
import com.delivery.dispatch.domain.DispatchStatus;
import com.delivery.dispatch.domain.StuckStateTimeoutPolicy;
import com.delivery.saga_orchestrator_service.entity.SagaInstance.SagaStatus;
import com.delivery.saga_orchestrator_service.entity.SagaOutboxEvent;
import com.delivery.saga_orchestrator_service.repository.SagaInstanceRepository;
import com.delivery.saga_orchestrator_service.repository.SagaOutboxEventRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.time.LocalDateTime;

/** Locks case before outbox, consistently with normal command production. */
@Service
@RequiredArgsConstructor
@Slf4j
public class SagaStuckStateTimeoutService {
    private final SagaInstanceRepository cases;
    private final SagaOutboxEventRepository outbox;
    private final MeterRegistry meters;

    @Transactional
    public void expire(Long orderId, SagaStatus expectedState, Duration timeout, int maxResends, String topic) {
        var saga = cases.findByOrderIdForUpdate(orderId).orElse(null);
        if (saga == null || saga.getStatus() != expectedState) return;
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime since = saga.getStuckLastResentAt() == null
                ? saga.getStateEnteredAt() : saga.getStuckLastResentAt();
        var state = DispatchStatus.valueOf(expectedState.name());
        var decision = StuckStateTimeoutPolicy.decide(state, since, saga.getStuckResendAttempts(),
                now, timeout, maxResends);
        if (decision == StuckStateTimeoutPolicy.Decision.WAIT) return;
        // The latest command for this state is the one whose reply is awaited.
        // Preserve its eventId, payload, topic, key and trace context exactly.
        var original = outbox.findFirstByAggregateIdAndTopicOrderByIdDesc(orderId.toString(), topic).orElse(null);
        var command = original == null ? null : outbox.findByIdForUpdate(original.getId()).orElseThrow();
        if (decision == StuckStateTimeoutPolicy.Decision.RESEND && command != null
                && command.getStatus() == SagaOutboxEvent.Status.IN_FLIGHT
                && command.getLeaseUntil() != null && command.getLeaseUntil().isAfter(now)) return;
        new DefaultStuckStateTimeoutUseCase().expire(state, since, saga.getStuckResendAttempts(), now,
                timeout, maxResends,
                () -> {
                    if (command == null) throw new IllegalStateException("Missing pending command for case=" + saga.getId());
                    command.setStatus(SagaOutboxEvent.Status.PENDING);
                    command.setAttempts(0);
                    command.setNextAttemptAt(now);
                    command.setSentAt(null);
                    command.setLeaseToken(null);
                    command.setLeaseUntil(null);
                    command.setLastError(null);
                    outbox.save(command);
                },
                attempts -> {
                    saga.setStuckResendAttempts(attempts);
                    saga.setStuckLastResentAt(now);
                    saga.addStep("STUCK_COMMAND_RESENT", topic, "{\"eventId\":\"" + command.getEventId() + "\"}");
                    cases.save(saga);
                },
                () -> {
                    saga.setStatus(SagaStatus.FAILED);
                    saga.setCompletedAt(now);
                    saga.addStep("STUCK_STATE_FAILED", "saga.stuck-state-failed", "{\"state\":\"" + expectedState + "\"}");
                    cases.save(saga);
                    TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                        @Override public void afterCommit() {
                            meters.counter("dispatch.stuck.state.failed", "state", expectedState.name()).increment();
                            log.error("Dispatch timeout exhausted; manual reconciliation required caseId={}, orderId={}, state={}, maxResends={}",
                                    saga.getId(), orderId, expectedState, maxResends);
                        }
                    });
                });
    }
}
