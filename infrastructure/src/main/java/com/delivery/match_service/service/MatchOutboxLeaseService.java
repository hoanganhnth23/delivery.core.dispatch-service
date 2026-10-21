package com.delivery.match_service.service;

import com.delivery.match_service.entity.MatchOutboxEvent;
import com.delivery.match_service.repository.MatchOutboxEventRepository;
import com.delivery.observability.SafeLog;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/** Short DB transactions for Match result outbox leases; Kafka I/O stays outside. */
@Service
public class MatchOutboxLeaseService {
    private final MatchOutboxEventRepository repository;

    public MatchOutboxLeaseService(MatchOutboxEventRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public List<MatchOutboxEvent> claim(int batchSize, UUID leaseToken, long leaseSeconds) {
        LocalDateTime until = LocalDateTime.now().plusSeconds(Math.max(5, leaseSeconds));
        List<MatchOutboxEvent> events = repository.lockNextClaimableBatch(Math.max(1, Math.min(batchSize, 500)));
        if (events == null || events.isEmpty()) {
            return List.of();
        }
        for (MatchOutboxEvent event : events) {
            event.setStatus(MatchOutboxEvent.Status.IN_FLIGHT);
            event.setLeaseToken(leaseToken);
            event.setLeaseUntil(until);
            repository.save(event);
        }
        return events;
    }

    @Transactional
    public boolean markSent(Long id, UUID token) {
        MatchOutboxEvent event = owned(id, token);
        if (event == null) return false;
        event.setStatus(MatchOutboxEvent.Status.SENT);
        event.setSentAt(LocalDateTime.now());
        event.setLastError(null);
        clearLease(event);
        repository.save(event);
        return true;
    }

    @Transactional
    public boolean markFailure(Long id, UUID token, Throwable failure, int maxAttempts) {
        MatchOutboxEvent event = owned(id, token);
        if (event == null) return false;
        int attempts = event.getAttempts() + 1;
        event.setAttempts(attempts);
        event.setLastError(abbreviate(SafeLog.exceptionMessage(failure)));
        if (attempts >= Math.max(1, Math.min(maxAttempts, 100))) {
            event.setStatus(MatchOutboxEvent.Status.DEAD);
        } else {
            long delay = Math.min(300L, 1L << Math.min(Math.max(attempts - 1, 0), 8));
            event.setStatus(MatchOutboxEvent.Status.PENDING);
            event.setNextAttemptAt(LocalDateTime.now().plusSeconds(delay));
        }
        clearLease(event);
        repository.save(event);
        return true;
    }

    private MatchOutboxEvent owned(Long id, UUID token) {
        if (id == null || token == null) return null;
        MatchOutboxEvent event = repository.findByIdForUpdate(id).orElse(null);
        return event != null && event.getStatus() == MatchOutboxEvent.Status.IN_FLIGHT
                && token.equals(event.getLeaseToken()) ? event : null;
    }

    private void clearLease(MatchOutboxEvent event) {
        event.setLeaseToken(null);
        event.setLeaseUntil(null);
    }

    private String abbreviate(String message) {
        if (message == null) return "Unknown Kafka publish failure";
        return message.length() <= 2000 ? message : message.substring(0, 2000);
    }
}
