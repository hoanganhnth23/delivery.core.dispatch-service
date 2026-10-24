package com.delivery.saga_orchestrator_service.service;

import com.delivery.observability.SafeLog;
import com.delivery.saga_orchestrator_service.entity.SagaOutboxEvent;
import com.delivery.saga_orchestrator_service.repository.SagaOutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/** Short DB transactions for Saga outbox leases; Kafka I/O stays outside. */
@Service
public class SagaOutboxLeaseService {
    private final SagaOutboxEventRepository repository;

    public SagaOutboxLeaseService(SagaOutboxEventRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public List<SagaOutboxEvent> claim(int batchSize, UUID leaseToken, long leaseSeconds) {
        if (leaseToken == null) {
            throw new IllegalArgumentException("Outbox lease token is required");
        }
        LocalDateTime leaseUntil = LocalDateTime.now().plusSeconds(Math.max(5, leaseSeconds));
        List<SagaOutboxEvent> events = repository.lockNextClaimableBatch(Math.max(1, Math.min(batchSize, 500)));
        if (events == null || events.isEmpty()) {
            return List.of();
        }
        for (SagaOutboxEvent event : events) {
            event.setStatus(SagaOutboxEvent.Status.IN_FLIGHT);
            event.setLeaseToken(leaseToken);
            event.setLeaseUntil(leaseUntil);
            repository.save(event);
        }
        return events;
    }

    @Transactional
    public boolean markSent(Long eventId, UUID leaseToken) {
        SagaOutboxEvent event = owned(eventId, leaseToken);
        if (event == null) return false;
        event.setStatus(SagaOutboxEvent.Status.SENT);
        event.setSentAt(LocalDateTime.now());
        event.setLastError(null);
        event.setLeaseToken(null);
        event.setLeaseUntil(null);
        repository.save(event);
        return true;
    }

    @Transactional
    public boolean markFailure(Long eventId, UUID leaseToken, Throwable failure, int maxAttempts) {
        SagaOutboxEvent event = owned(eventId, leaseToken);
        if (event == null) return false;
        int attempts = event.getAttempts() + 1;
        event.setAttempts(attempts);
        event.setLastError(abbreviate(SafeLog.exceptionMessage(failure)));
        if (attempts >= Math.max(1, Math.min(maxAttempts, 100))) {
            event.setStatus(SagaOutboxEvent.Status.DEAD);
        } else {
            long delay = Math.min(300L, 1L << Math.min(Math.max(attempts - 1, 0), 8));
            event.setStatus(SagaOutboxEvent.Status.PENDING);
            event.setNextAttemptAt(LocalDateTime.now().plusSeconds(delay));
        }
        event.setLeaseToken(null);
        event.setLeaseUntil(null);
        repository.save(event);
        return true;
    }

    private SagaOutboxEvent owned(Long eventId, UUID leaseToken) {
        if (eventId == null || leaseToken == null) return null;
        SagaOutboxEvent event = repository.findByIdForUpdate(eventId).orElse(null);
        return event != null && event.getStatus() == SagaOutboxEvent.Status.IN_FLIGHT
                && leaseToken.equals(event.getLeaseToken()) ? event : null;
    }

    private String abbreviate(String message) {
        if (message == null) return "Unknown Kafka publish failure";
        return message.length() <= 2000 ? message : message.substring(0, 2000);
    }
}
