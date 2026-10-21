package com.delivery.match_service.service;

import com.delivery.match.domain.batch.BatchPoolPolicy;

import com.delivery.match.domain.command.MatchCommandPolicy;
import com.delivery.match_service.repository.DispatchPoolItemRepository;
import com.delivery.match_service.entity.DispatchPoolItem;
import com.delivery.match_service.dto.event.FindShipperEvent;
import com.delivery.match_service.dto.event.MatchingDecisionTraceEvent;
import com.delivery.match_service.dto.event.ShipperFoundEvent;
import com.delivery.match_service.dto.event.ShipperNotFoundEvent;
import com.delivery.match_service.entity.MatchCancellationTombstone;
import com.delivery.match_service.entity.MatchCommand;
import com.delivery.match_service.entity.MatchOutboxEvent;
import com.delivery.match_service.repository.MatchCancellationTombstoneRepository;
import com.delivery.match_service.repository.MatchCommandRepository;
import com.delivery.match_service.repository.MatchOutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Owns Match's durable inbox and result outbox boundary. Candidate selection
 * remains a Redis/Settlement operation, but its first selected payload is
 * durably staged before the Redis offer reservation so a crash/replay cannot
 * switch to another shipper.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class MatchCommandStore {

    public enum CommandMode {
        PROCESS,
        RESUME_CANDIDATE,
        TERMINAL
    }

    public record CommandDecision(CommandMode mode, ShipperFoundEvent stagedCandidate) {
        public static CommandDecision process() {
            return new CommandDecision(CommandMode.PROCESS, null);
        }

        public static CommandDecision resume(ShipperFoundEvent candidate) {
            return new CommandDecision(CommandMode.RESUME_CANDIDATE, candidate);
        }

        public static CommandDecision terminal() {
            return new CommandDecision(CommandMode.TERMINAL, null);
        }
    }

    public record TerminalResultDecision(boolean staged, ShipperFoundEvent stagedCandidate) {
        public static TerminalResultDecision persisted() {
            return new TerminalResultDecision(true, null);
        }

        public static TerminalResultDecision resume(ShipperFoundEvent candidate) {
            return new TerminalResultDecision(false, candidate);
        }

        public static TerminalResultDecision terminal() {
            return new TerminalResultDecision(false, null);
        }
    }

    private final MatchCommandRepository commandRepository;
    private final MatchCancellationTombstoneRepository cancellationTombstoneRepository;
    private final MatchOutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final Tracer tracer;
    private final DispatchPoolItemRepository poolRepository;

    /**
     * Persists/validates the command before Match reads Redis or calls
     * Settlement. Exact replays resume incomplete work; a conflicting payload
     * with the same source identity is poison and must not overwrite truth.
     */
    @Transactional(isolation = Isolation.SERIALIZABLE)
    public CommandDecision acceptFindCommand(
            String topic,
            String rawPayload,
            FindShipperEvent command) {
        requireFindCommand(command);
        requireText(topic, "topic");
        requireText(rawPayload, "raw payload");

        UUID commandId = command.getEventId();
        UUID matchingSessionId = matchingSessionId(command);
        String fingerprint = sha256(rawPayload);
        MatchCancellationTombstone tombstone = cancellationTombstoneRepository
                .findByDeliveryAndSessionForUpdate(command.getDeliveryId(), matchingSessionId)
                .orElse(null);
        if (tombstone != null) {
            return acceptFindAfterCancellation(tombstone, topic, rawPayload, command, fingerprint,
                    matchingSessionId);
        }

        MatchCommand existing = commandRepository.findByEventIdForUpdate(commandId).orElse(null);
        if (existing == null) {
            MatchCommand generationOwner = commandRepository
                    .findByDeliveryAndSessionForUpdate(command.getDeliveryId(), matchingSessionId)
                    .orElse(null);
            MatchCommandPolicy.requireUnownedGeneration(generationOwner != null);
            MatchCommand created = new MatchCommand(
                    commandId,
                    topic,
                    command.getOrderId(),
                    command.getDeliveryId(),
                    matchingSessionId,
                    rawPayload,
                    fingerprint);
            commandRepository.saveAndFlush(created);

            // SERIALIZABLE prevents an unnoticed stop/find first-arrival race.
            // The second read also covers a stop that committed after the first
            // absence check but before the command insert.
            MatchCancellationTombstone afterInsert = cancellationTombstoneRepository
                    .findByDeliveryAndSessionForUpdate(command.getDeliveryId(), matchingSessionId)
                    .orElse(null);
            if (afterInsert != null) {
                assertTombstoneTarget(afterInsert, command, matchingSessionId);
                cancelIfUnpublished(created);
                return CommandDecision.terminal();
            }
            return CommandDecision.process();
        }

        assertExactCommandReplay(existing, topic, command, fingerprint, matchingSessionId);
        return switch (MatchCommandPolicy.next(status(existing))) {
            case PROCESS -> CommandDecision.process();
            case RESUME -> CommandDecision.resume(readCandidate(existing));
            case TERMINAL -> CommandDecision.terminal();
        };
    }

    /**
     * Records a generation-scoped cancellation before Redis is touched. The
     * tombstone is also a durable receipt when Kafka delivers stop-matching
     * before the corresponding find command on its separate topic.
     */
    @Transactional(isolation = Isolation.SERIALIZABLE)
    public void recordStopMatching(
            UUID stopEventId,
            Long orderId,
            Long deliveryId,
            UUID matchingSessionId,
            String rawPayload) {
        requireStopCommand(stopEventId, orderId, deliveryId, matchingSessionId, rawPayload);
        String fingerprint = sha256(rawPayload);

        MatchCancellationTombstone sameEvent = cancellationTombstoneRepository
                .findById(stopEventId)
                .orElse(null);
        if (sameEvent != null) {
            assertExactStopReplay(sameEvent, orderId, deliveryId, matchingSessionId, fingerprint);
        }

        MatchCancellationTombstone tombstone = cancellationTombstoneRepository
                .findByDeliveryAndSessionForUpdate(deliveryId, matchingSessionId)
                .orElse(null);
        if (tombstone == null) {
            tombstone = new MatchCancellationTombstone(
                    stopEventId, orderId, deliveryId, matchingSessionId, fingerprint);
            cancellationTombstoneRepository.saveAndFlush(tombstone);
        } else {
            assertTombstoneIdentity(tombstone, orderId, deliveryId, matchingSessionId);
        }

        MatchCommand command = commandRepository
                .findByDeliveryAndSessionForUpdate(deliveryId, matchingSessionId)
                .orElse(null);
        if (command != null) {
            if (!Objects.equals(command.getOrderId(), orderId)) {
                throw new IllegalArgumentException(
                        "Stop-matching orderId conflicts with the durable Match command");
            }
            cancelIfUnpublished(command);
        }
        retirePooledGeneration(deliveryId, matchingSessionId);
    }

    /**
     * A stopped generation must leave the batch pool too. An ASSIGNED item was
     * already offered as part of a batch; Delivery's cancellation owns it.
     */
    private void retirePooledGeneration(Long deliveryId, UUID matchingSessionId) {
        poolRepository.findByDeliveryAndSessionForUpdate(deliveryId, matchingSessionId)
                .filter(item -> BatchPoolPolicy.retiredByStop(
                        BatchPoolPolicy.State.valueOf(item.getState().name())))
                .ifPresent(item -> {
                    item.setState(DispatchPoolItem.State.CANCELLED);
                    item.setClaimedRoundId(null);
                    item.setUpdatedAt(LocalDateTime.now());
                    poolRepository.save(item);
                });
    }

    private CommandDecision acceptFindAfterCancellation(
            MatchCancellationTombstone tombstone,
            String topic,
            String rawPayload,
            FindShipperEvent command,
            String fingerprint,
            UUID matchingSessionId) {
        assertTombstoneTarget(tombstone, command, matchingSessionId);
        MatchCommand existing = commandRepository.findByEventIdForUpdate(command.getEventId()).orElse(null);
        if (existing != null) {
            assertExactCommandReplay(existing, topic, command, fingerprint, matchingSessionId);
            cancelIfUnpublished(existing);
            return CommandDecision.terminal();
        }

        MatchCommand generationOwner = commandRepository
                .findByDeliveryAndSessionForUpdate(command.getDeliveryId(), matchingSessionId)
                .orElse(null);
        if (generationOwner != null) {
            throw new IllegalArgumentException(
                    "Match matchingSessionId is already owned by a different command eventId");
        }

        MatchCommand cancelled = new MatchCommand(
                command.getEventId(),
                topic,
                command.getOrderId(),
                command.getDeliveryId(),
                matchingSessionId,
                rawPayload,
                fingerprint);
        markCancelled(cancelled);
        commandRepository.saveAndFlush(cancelled);
        return CommandDecision.terminal();
    }

    /**
     * The first process that stages a candidate wins. Parallel exact command
     * replays may have read a different volatile GEO view, but they must resume
     * the stored candidate and never publish a second matching outcome.
     */
    @Transactional
    public ShipperFoundEvent stageCandidate(UUID commandId, ShipperFoundEvent proposedCandidate) {
        MatchCommand command = requireCommandForUpdate(commandId);
        if (command.getStatus() == MatchCommand.Status.RESULT_STAGED
                || command.getStatus() == MatchCommand.Status.CANCELLED) {
            return null;
        }
        if (command.getStatus() == MatchCommand.Status.CANDIDATE_STAGED) {
            return readCandidate(command);
        }

        requireFoundResult(command, proposedCandidate);
        command.setCandidatePayload(serialize(proposedCandidate));
        command.setStatus(MatchCommand.Status.CANDIDATE_STAGED);
        command.setUpdatedAt(LocalDateTime.now());
        commandRepository.save(command);
        return proposedCandidate;
    }

    /**
     * Commits the stable shipper.found payload and its outbox row atomically.
     * A true return means this command owns a durable found result, whether
     * this invocation staged it or an exact concurrent replay already did.
     * Only a false return may release the Redis offer: cancellation or a
     * different terminal outcome won while Match was reserving the candidate.
     */
    @Transactional
    public boolean stageFoundResult(UUID commandId, ShipperFoundEvent candidate) {
        MatchCommand command = requireCommandForUpdate(commandId);
        if (command.getStatus() == MatchCommand.Status.CANCELLED) {
            return false;
        }
        if (command.getStatus() == MatchCommand.Status.RESULT_STAGED) {
            // Two Kafka partitions can carry an exact duplicate. Both workers
            // may reserve the same idempotent Redis offer, but the worker that
            // observes the result already staged must not release the offer
            // owned by that same delivery/session. A not-found terminal has no
            // candidate payload, so it remains a losing reservation and must
            // be released by the caller.
            if (command.getCandidatePayload() == null || command.getCandidatePayload().isBlank()) {
                return false;
            }
            ShipperFoundEvent storedCandidate = readCandidate(command);
            requireSameCandidate(storedCandidate, candidate);
            return true;
        }
        if (command.getStatus() != MatchCommand.Status.CANDIDATE_STAGED) {
            throw new IllegalStateException("Match found result requires a staged candidate");
        }

        ShipperFoundEvent storedCandidate = readCandidate(command);
        requireFoundResult(command, storedCandidate);
        requireSameCandidate(storedCandidate, candidate);
        persistResultOutbox(command, storedCandidate.getEventId(), "shipper.found",
                command.getOrderId().toString(), command.getCandidatePayload());
        markResultStaged(command);
        return true;
    }

    /**
     * Commits shipper.not-found atomically with its durable outbox. A normal
     * exhaustion does not overwrite a concurrently staged candidate; a deadline
     * is authoritative and may discard an unreserved candidate.
     */
    @Transactional
    public TerminalResultDecision stageNotFoundResult(
            UUID commandId,
            ShipperNotFoundEvent outcome,
            boolean deadlineTerminal) {
        MatchCommand command = requireCommandForUpdate(commandId);
        if (command.getStatus() == MatchCommand.Status.CANCELLED
                || command.getStatus() == MatchCommand.Status.RESULT_STAGED) {
            return TerminalResultDecision.terminal();
        }
        if (command.getStatus() == MatchCommand.Status.CANDIDATE_STAGED && !deadlineTerminal) {
            return TerminalResultDecision.resume(readCandidate(command));
        }
        if (command.getStatus() == MatchCommand.Status.CANDIDATE_STAGED) {
            command.setCandidatePayload(null);
            command.setStatus(MatchCommand.Status.PENDING);
        }

        requireNotFoundResult(command, outcome);
        String payload = serialize(outcome);
        persistResultOutbox(command, outcome.getEventId(), "shipper.not-found",
                command.getOrderId().toString(), payload);
        markResultStaged(command);
        return TerminalResultDecision.persisted();
    }

    /**
     * Persist a best-effort, read-only explanation after the business result
     * is durable. The trace has its own deterministic event identity and is
     * relayed through the same outbox, but it never gates shipper assignment.
     */
    @Transactional
    public void stageDecisionTrace(UUID commandId, MatchingDecisionTraceEvent trace) {
        if (trace == null || trace.getEventId() == null
                || trace.getCommandEventId() == null
                || !Objects.equals(commandId, trace.getCommandEventId())) {
            throw new IllegalArgumentException("Matching decision trace identity is invalid");
        }
        MatchCommand command = requireCommandForUpdate(commandId);
        if (command.getStatus() == MatchCommand.Status.CANCELLED) {
            return;
        }
        if (command.getStatus() != MatchCommand.Status.RESULT_STAGED) {
            throw new IllegalStateException("Matching decision trace requires a durable Match result");
        }
        if (!Objects.equals(command.getOrderId(), trace.getOrderId())
                || !Objects.equals(command.getDeliveryId(), trace.getDeliveryId())
                || !Objects.equals(command.getMatchingSessionId().toString(), trace.getMatchingSessionId())) {
            throw new IllegalArgumentException("Matching decision trace does not match the durable command");
        }
        persistDecisionTraceOutbox(command, trace.getEventId().toString(), serialize(trace));
    }

    /** Clear an unreserved candidate after a Redis reservation race so retry can search again. */
    @Transactional
    public void clearStagedCandidate(UUID commandId) {
        MatchCommand command = requireCommandForUpdate(commandId);
        if (command.getStatus() == MatchCommand.Status.CANDIDATE_STAGED) {
            command.setCandidatePayload(null);
            command.setStatus(MatchCommand.Status.PENDING);
            command.setUpdatedAt(LocalDateTime.now());
            commandRepository.save(command);
        }
    }

    /** Persist local cancellation for a command already accepted by Match. */
    @Transactional
    public void cancelCommand(UUID commandId) {
        commandRepository.findByEventIdForUpdate(commandId).ifPresent(this::cancelIfUnpublished);
    }

    private void cancelIfUnpublished(MatchCommand command) {
        boolean cancelledUnsentResult = false;
        if (command.getStatus() == MatchCommand.Status.RESULT_STAGED) {
            List<MatchOutboxEvent> outboxEvents =
                    outboxRepository.findByCommandEventIdForUpdate(command.getEventId());
            for (MatchOutboxEvent event : outboxEvents) {
                if (MatchCommandPolicy.suppressible(
                        MatchCommandPolicy.OutboxStatus.valueOf(event.getStatus().name()))) {
                    event.setStatus(MatchOutboxEvent.Status.CANCELLED);
                    event.setLastError("Cancelled before Match result relay");
                    cancelledUnsentResult = true;
                }
            }
        }
        if (!MatchCommandPolicy.cancels(status(command), cancelledUnsentResult)) {
            return;
        }
        markCancelled(command);
        commandRepository.save(command);
    }

    private void markCancelled(MatchCommand command) {
        LocalDateTime now = LocalDateTime.now();
        command.setStatus(MatchCommand.Status.CANCELLED);
        command.setUpdatedAt(now);
        command.setCompletedAt(now);
    }

    private MatchCommand requireCommandForUpdate(UUID commandId) {
        if (commandId == null) {
            throw new IllegalArgumentException("Match command eventId is required");
        }
        return commandRepository.findByEventIdForUpdate(commandId)
                .orElseThrow(() -> new EntityNotFoundException(
                        "No durable Match command exists for eventId=" + commandId));
    }

    private void persistResultOutbox(
            MatchCommand command,
            String eventIdValue,
            String eventType,
            String eventKey,
            String payload) {
        persistOutbox(command, eventIdValue, eventType, eventType, eventKey, payload);
    }

    private void persistDecisionTraceOutbox(
            MatchCommand command,
            String eventIdValue,
            String payload) {
        persistOutbox(command, eventIdValue, MatchingDecisionTraceEvent.EVENT_TYPE,
                MatchingDecisionTraceEvent.TOPIC, command.getOrderId().toString(), payload);
    }

    private void persistOutbox(
            MatchCommand command,
            String eventIdValue,
            String eventType,
            String topic,
            String eventKey,
            String payload) {
        UUID eventId = parseUuid(eventIdValue, "result eventId");
        MatchOutboxEvent existing = outboxRepository.findByEventId(eventId).orElse(null);
        if (existing != null) {
            if (!Objects.equals(existing.getCommandEventId(), command.getEventId())
                    || !Objects.equals(existing.getEventType(), eventType)
                    || !Objects.equals(existing.getTopic(), topic)
                    || !Objects.equals(existing.getEventKey(), eventKey)
                    || !Objects.equals(existing.getPayload(), payload)) {
                throw new IllegalStateException("Match result eventId conflicts with durable outbox payload");
            }
            return;
        }

        LocalDateTime now = LocalDateTime.now();
        MatchOutboxEvent event = new MatchOutboxEvent();
        event.setEventId(eventId);
        event.setCommandEventId(command.getEventId());
        event.setAggregateId(command.getOrderId().toString());
        event.setEventType(eventType);
        event.setTopic(topic);
        event.setEventKey(eventKey);
        event.setPayload(payload);
        event.setTraceparent(currentTraceparent());
        event.setStatus(MatchOutboxEvent.Status.PENDING);
        event.setAttempts(0);
        event.setNextAttemptAt(now);
        event.setCreatedAt(now);
        outboxRepository.save(event);
    }

    private void markResultStaged(MatchCommand command) {
        LocalDateTime now = LocalDateTime.now();
        command.setStatus(MatchCommand.Status.RESULT_STAGED);
        command.setUpdatedAt(now);
        command.setCompletedAt(now);
        commandRepository.save(command);
    }

    private void assertExactCommandReplay(
            MatchCommand existing,
            String topic,
            FindShipperEvent command,
            String fingerprint,
            UUID matchingSessionId) {
        MatchCommandPolicy.requireExactReplay(
                new MatchCommandPolicy.CommandIdentity(existing.getTopic(), existing.getOrderId(),
                        existing.getDeliveryId(), existing.getMatchingSessionId(), existing.getPayloadFingerprint()),
                new MatchCommandPolicy.CommandIdentity(topic, command.getOrderId(), command.getDeliveryId(),
                        matchingSessionId, fingerprint));
    }

    private void assertTombstoneTarget(
            MatchCancellationTombstone tombstone,
            FindShipperEvent command,
            UUID matchingSessionId) {
        assertTombstoneIdentity(tombstone, command.getOrderId(), command.getDeliveryId(), matchingSessionId);
    }

    private void assertTombstoneIdentity(
            MatchCancellationTombstone tombstone,
            Long orderId,
            Long deliveryId,
            UUID matchingSessionId) {
        MatchCommandPolicy.requireTombstoneIdentity(tombstone.getOrderId(), tombstone.getDeliveryId(),
                tombstone.getMatchingSessionId(), orderId, deliveryId, matchingSessionId);
    }

    private void assertExactStopReplay(
            MatchCancellationTombstone tombstone,
            Long orderId,
            Long deliveryId,
            UUID matchingSessionId,
            String fingerprint) {
        assertTombstoneIdentity(tombstone, orderId, deliveryId, matchingSessionId);
        MatchCommandPolicy.requireExactStopReplay(tombstone.getPayloadFingerprint(), fingerprint);
    }

    private ShipperFoundEvent readCandidate(MatchCommand command) {
        if (command.getCandidatePayload() == null || command.getCandidatePayload().isBlank()) {
            throw new IllegalStateException("Staged Match candidate payload is missing");
        }
        try {
            ShipperFoundEvent candidate = objectMapper.readValue(
                    command.getCandidatePayload(), ShipperFoundEvent.class);
            requireFoundResult(command, candidate);
            return candidate;
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Cannot read staged Match candidate payload", exception);
        }
    }

    private void requireFindCommand(FindShipperEvent command) {
        if (command == null || command.getEventId() == null
                || command.getOrderId() == null || command.getOrderId() <= 0
                || command.getDeliveryId() == null || command.getDeliveryId() <= 0) {
            throw new IllegalArgumentException(
                    "Match command eventId and positive order/delivery IDs are required");
        }
    }

    private void requireFoundResult(MatchCommand command, ShipperFoundEvent event) {
        if (event == null
                || event.getDeliveryId() == null || !event.getDeliveryId().equals(command.getDeliveryId())
                || event.getOrderId() == null || !event.getOrderId().equals(command.getOrderId())
                || event.getEventId() == null || event.getEventId().isBlank()
                || event.getMatchingSessionId() == null
                || !command.getMatchingSessionId().toString().equals(event.getMatchingSessionId())
                || event.getAvailableShippers() == null || event.getAvailableShippers().size() != 1
                || event.getAvailableShippers().get(0) == null
                || event.getAvailableShippers().get(0).getShipperId() == null
                || event.getAvailableShippers().get(0).getShipperId() <= 0) {
            throw new IllegalArgumentException(
                    "Match found result must contain the command identity and exactly one positive shipper");
        }
        parseUuid(event.getEventId(), "shipper.found eventId");
    }

    private void requireNotFoundResult(MatchCommand command, ShipperNotFoundEvent event) {
        if (event == null
                || event.getDeliveryId() == null || !event.getDeliveryId().equals(command.getDeliveryId())
                || event.getOrderId() == null || !event.getOrderId().equals(command.getOrderId())
                || event.getEventId() == null || event.getEventId().isBlank()
                || event.getMatchingSessionId() == null
                || !command.getMatchingSessionId().toString().equals(event.getMatchingSessionId())) {
            throw new IllegalArgumentException(
                    "Match not-found result must contain the command and matching session identity");
        }
        parseUuid(event.getEventId(), "shipper.not-found eventId");
    }

    private void requireSameCandidate(ShipperFoundEvent stored, ShipperFoundEvent provided) {
        if (provided == null
                || !Objects.equals(stored.getEventId(), provided.getEventId())
                || !Objects.equals(stored.getDeliveryId(), provided.getDeliveryId())
                || !Objects.equals(stored.getOrderId(), provided.getOrderId())
                || !Objects.equals(stored.getMatchingSessionId(), provided.getMatchingSessionId())
                || stored.getAvailableShippers() == null || provided.getAvailableShippers() == null
                || stored.getAvailableShippers().isEmpty() || provided.getAvailableShippers().isEmpty()
                || !Objects.equals(stored.getAvailableShippers().get(0).getShipperId(),
                        provided.getAvailableShippers().get(0).getShipperId())) {
            throw new IllegalArgumentException("Match result does not match the staged candidate");
        }
    }

    private String serialize(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Cannot serialize durable Match payload", exception);
        }
    }

    private UUID parseUuid(String value, String field) {
        try {
            return UUID.fromString(value);
        } catch (Exception exception) {
            throw new IllegalArgumentException(field + " must be a UUID", exception);
        }
    }

    private String sha256(String payload) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(payload.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(64);
            for (byte value : digest) {
                hex.append(String.format("%02x", value));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private String currentTraceparent() {
        Span span = tracer.currentSpan();
        if (span == null || span.context() == null) {
            return null;
        }
        return "00-" + span.context().traceId() + "-" + span.context().spanId()
                + "-" + (Boolean.TRUE.equals(span.context().sampled()) ? "01" : "00");
    }

    private void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
    }

    private UUID matchingSessionId(FindShipperEvent command) {
        // Existing V1 find commands did not carry a separate session. Their
        // command event ID is the only safe generation identity during rollout.
        return com.delivery.match.domain.single.SingleOfferPolicy.sessionId(
                command.getEventId(), command.getMatchingSessionId());
    }

    private void requireStopCommand(
            UUID stopEventId,
            Long orderId,
            Long deliveryId,
            UUID matchingSessionId,
            String rawPayload) {
        MatchCommandPolicy.requireStopCommand(stopEventId, orderId, deliveryId, matchingSessionId, rawPayload);
    }

    private static MatchCommandPolicy.Status status(MatchCommand command) {
        return MatchCommandPolicy.Status.valueOf(command.getStatus().name());
    }
}
