package com.delivery.match_service.service;

import com.delivery.match_service.MatchServiceApplication;
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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(classes = MatchServiceApplication.class, properties = {
        "spring.kafka.listener.auto-startup=false",
        "app.outbox.relay-enabled=false"
})
class MatchCommandStoreIntegrationTest {

    @Autowired
    private MatchCommandStore store;

    @Autowired
    private MatchCommandRepository commandRepository;

    @Autowired
    private MatchCancellationTombstoneRepository cancellationTombstoneRepository;

    @Autowired
    private MatchOutboxEventRepository outboxRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void clean() {
        outboxRepository.deleteAll();
        commandRepository.deleteAll();
        cancellationTombstoneRepository.deleteAll();
    }

    @Test
    void persistsCandidateAndResultOutboxThenTreatsExactReplayAsTerminal() throws Exception {
        FindShipperEvent command = command(UUID.fromString("11111111-1111-1111-1111-111111111111"));
        String raw = objectMapper.writeValueAsString(command);

        assertThat(store.acceptFindCommand("saga.command.find-shipper", raw, command).mode())
                .isEqualTo(MatchCommandStore.CommandMode.PROCESS);
        ShipperFoundEvent candidate = found(command, 901L);
        assertThat(store.stageCandidate(command.getEventId(), candidate).getAvailableShippers().get(0).getShipperId())
                .isEqualTo(901L);
        assertThat(store.stageFoundResult(command.getEventId(), candidate)).isTrue();

        MatchCommand persisted = commandRepository.findById(command.getEventId()).orElseThrow();
        assertThat(persisted.getStatus()).isEqualTo(MatchCommand.Status.RESULT_STAGED);
        assertThat(outboxRepository.findAll()).singleElement().satisfies(outbox -> {
            assertThat(outbox.getStatus()).isEqualTo(MatchOutboxEvent.Status.PENDING);
            assertThat(outbox.getEventId()).isEqualTo(UUID.fromString(candidate.getEventId()));
            assertThat(outbox.getCommandEventId()).isEqualTo(command.getEventId());
            assertThat(outbox.getTopic()).isEqualTo("shipper.found");
            assertThat(outbox.getPayload()).contains(candidate.getEventId());
        });
        assertThat(store.acceptFindCommand("saga.command.find-shipper", raw, command).mode())
                .isEqualTo(MatchCommandStore.CommandMode.TERMINAL);
        assertThat(outboxRepository.count()).isOne();
    }

    @Test
    void persistsReadOnlyDecisionTraceAfterTheBusinessResultWithoutReplacingIt() throws Exception {
        FindShipperEvent command = command(UUID.fromString("12121212-1212-1212-1212-121212121212"));
        String raw = objectMapper.writeValueAsString(command);
        store.acceptFindCommand("saga.command.find-shipper", raw, command);
        ShipperFoundEvent candidate = found(command, 906L);
        store.stageCandidate(command.getEventId(), candidate);
        store.stageFoundResult(command.getEventId(), candidate);

        MatchingDecisionTraceEvent trace = new MatchingDecisionTraceEvent();
        trace.setEventId(UUID.fromString("13131313-1313-1313-1313-131313131313"));
        trace.setCommandEventId(command.getEventId());
        trace.setMatchingSessionId(command.getMatchingSessionId().toString());
        trace.setOrderId(command.getOrderId());
        trace.setDeliveryId(command.getDeliveryId());
        trace.setDecision("SHIPPER_SELECTED");
        trace.setSelectedShipperId(906L);

        store.stageDecisionTrace(command.getEventId(), trace);
        store.stageDecisionTrace(command.getEventId(), trace);

        assertThat(outboxRepository.findAll()).hasSize(2);
        assertThat(outboxRepository.findByEventId(trace.getEventId())).get()
                .satisfies(outbox -> {
                    assertThat(outbox.getEventType()).isEqualTo(MatchingDecisionTraceEvent.EVENT_TYPE);
                    assertThat(outbox.getTopic()).isEqualTo(MatchingDecisionTraceEvent.TOPIC);
                    assertThat(outbox.getCommandEventId()).isEqualTo(command.getEventId());
                    assertThat(outbox.getPayload()).contains("SHIPPER_SELECTED");
                });
        assertThat(commandRepository.findById(command.getEventId()).orElseThrow().getStatus())
                .isEqualTo(MatchCommand.Status.RESULT_STAGED);
    }

    @Test
    void conflictingPayloadWithSameCommandIdentityFailsClosed() throws Exception {
        FindShipperEvent original = command(UUID.fromString("22222222-2222-2222-2222-222222222222"));
        String rawOriginal = objectMapper.writeValueAsString(original);
        store.acceptFindCommand("saga.command.find-shipper", rawOriginal, original);

        FindShipperEvent contradictory = command(original.getEventId());
        contradictory.setDeliveryAddress("Changed address must not overwrite durable command");
        String rawContradictory = objectMapper.writeValueAsString(contradictory);

        assertThatThrownBy(() -> store.acceptFindCommand(
                "saga.command.find-shipper", rawContradictory, contradictory))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("contradictory payload");
        assertThat(commandRepository.count()).isOne();
        assertThat(outboxRepository.count()).isZero();
    }

    @Test
    void replayResumesTheSameCandidateAfterCrashBeforeResultCommit() throws Exception {
        FindShipperEvent command = command(UUID.fromString("33333333-3333-3333-3333-333333333333"));
        String raw = objectMapper.writeValueAsString(command);
        store.acceptFindCommand("saga.command.find-shipper", raw, command);
        ShipperFoundEvent selected = found(command, 902L);
        store.stageCandidate(command.getEventId(), selected);

        MatchCommandStore.CommandDecision replay =
                store.acceptFindCommand("saga.command.find-shipper", raw, command);
        assertThat(replay.mode()).isEqualTo(MatchCommandStore.CommandMode.RESUME_CANDIDATE);
        assertThat(replay.stagedCandidate().getAvailableShippers().get(0).getShipperId())
                .isEqualTo(902L);
        assertThat(store.stageFoundResult(command.getEventId(), replay.stagedCandidate())).isTrue();
        assertThat(outboxRepository.findAll()).singleElement()
                .extracting(MatchOutboxEvent::getTopic)
                .isEqualTo("shipper.found");
    }

    @Test
    void deadlineTerminalDiscardsOnlyUnreservedCandidateAndStagesNotFound() throws Exception {
        FindShipperEvent command = command(UUID.fromString("44444444-4444-4444-4444-444444444444"));
        String raw = objectMapper.writeValueAsString(command);
        store.acceptFindCommand("saga.command.find-shipper", raw, command);
        store.stageCandidate(command.getEventId(), found(command, 903L));

        ShipperNotFoundEvent terminal = new ShipperNotFoundEvent(command.getDeliveryId(), command.getOrderId(), 2);
        terminal.setEventId("66666666-6666-6666-6666-666666666666");
        terminal.setMatchingSessionId(command.getMatchingSessionId().toString());
        MatchCommandStore.TerminalResultDecision result =
                store.stageNotFoundResult(command.getEventId(), terminal, true);

        assertThat(result.staged()).isTrue();
        assertThat(commandRepository.findById(command.getEventId()).orElseThrow().getStatus())
                .isEqualTo(MatchCommand.Status.RESULT_STAGED);
        assertThat(outboxRepository.findAll()).singleElement()
                .extracting(MatchOutboxEvent::getTopic)
                .isEqualTo("shipper.not-found");
    }

    @Test
    void stopMatchingSuppressesAnUnsentDurableResult() throws Exception {
        FindShipperEvent command = command(UUID.fromString("55555555-5555-5555-5555-555555555555"));
        String raw = objectMapper.writeValueAsString(command);
        store.acceptFindCommand("saga.command.find-shipper", raw, command);
        ShipperFoundEvent candidate = found(command, 904L);
        store.stageCandidate(command.getEventId(), candidate);
        store.stageFoundResult(command.getEventId(), candidate);

        String stopPayload = stopPayload(
                UUID.fromString("99999999-9999-9999-9999-999999999999"), command);
        store.recordStopMatching(
                UUID.fromString("99999999-9999-9999-9999-999999999999"),
                command.getOrderId(), command.getDeliveryId(), command.getMatchingSessionId(), stopPayload);

        assertThat(commandRepository.findById(command.getEventId()).orElseThrow().getStatus())
                .isEqualTo(MatchCommand.Status.CANCELLED);
        assertThat(outboxRepository.findAll()).singleElement()
                .extracting(MatchOutboxEvent::getStatus)
                .isEqualTo(MatchOutboxEvent.Status.CANCELLED);
    }

    @Test
    void stopBeforeFindCreatesADurableFenceWithoutBlockingANewerRematchGeneration() throws Exception {
        FindShipperEvent stopped = command(UUID.fromString("88888888-8888-8888-8888-888888888888"));
        UUID stopEventId = UUID.fromString("99999999-9999-9999-9999-999999999999");
        String stopPayload = stopPayload(stopEventId, stopped);

        store.recordStopMatching(stopEventId, stopped.getOrderId(), stopped.getDeliveryId(),
                stopped.getMatchingSessionId(), stopPayload);

        assertThat(store.acceptFindCommand("saga.command.find-shipper",
                objectMapper.writeValueAsString(stopped), stopped).mode())
                .isEqualTo(MatchCommandStore.CommandMode.TERMINAL);
        assertThat(commandRepository.findById(stopped.getEventId()).orElseThrow().getStatus())
                .isEqualTo(MatchCommand.Status.CANCELLED);
        assertThat(cancellationTombstoneRepository.findAll()).singleElement()
                .extracting(MatchCancellationTombstone::getMatchingSessionId)
                .isEqualTo(stopped.getMatchingSessionId());

        FindShipperEvent rematch = command(UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"));
        rematch.setMatchingSessionId(UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"));
        assertThat(store.acceptFindCommand("saga.command.find-shipper",
                objectMapper.writeValueAsString(rematch), rematch).mode())
                .isEqualTo(MatchCommandStore.CommandMode.PROCESS);
        assertThat(commandRepository.findById(rematch.getEventId()).orElseThrow().getStatus())
                .isEqualTo(MatchCommand.Status.PENDING);
    }

    @Test
    void legacyFindWithoutSessionUsesItsCommandIdentityAsTheRolloutGeneration() throws Exception {
        UUID eventId = UUID.fromString("abababab-abab-abab-abab-abababababab");
        FindShipperEvent legacy = command(eventId);
        legacy.setMatchingSessionId(null);

        assertThat(store.acceptFindCommand("saga.command.find-shipper",
                objectMapper.writeValueAsString(legacy), legacy).mode())
                .isEqualTo(MatchCommandStore.CommandMode.PROCESS);

        assertThat(commandRepository.findById(eventId).orElseThrow().getMatchingSessionId())
                .isEqualTo(eventId);
    }

    @Test
    void staleStopCancelsOnlyItsTargetGeneration() throws Exception {
        FindShipperEvent oldGeneration = command(UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc"));
        FindShipperEvent newerGeneration = command(UUID.fromString("dddddddd-dddd-dddd-dddd-dddddddddddd"));
        newerGeneration.setMatchingSessionId(UUID.fromString("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"));
        store.acceptFindCommand("saga.command.find-shipper", objectMapper.writeValueAsString(oldGeneration), oldGeneration);
        store.acceptFindCommand("saga.command.find-shipper", objectMapper.writeValueAsString(newerGeneration), newerGeneration);

        UUID stopEventId = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");
        store.recordStopMatching(stopEventId, oldGeneration.getOrderId(), oldGeneration.getDeliveryId(),
                oldGeneration.getMatchingSessionId(), stopPayload(stopEventId, oldGeneration));

        assertThat(commandRepository.findById(oldGeneration.getEventId()).orElseThrow().getStatus())
                .isEqualTo(MatchCommand.Status.CANCELLED);
        assertThat(commandRepository.findById(newerGeneration.getEventId()).orElseThrow().getStatus())
                .isEqualTo(MatchCommand.Status.PENDING);
    }

    @Test
    @Transactional
    void orderedOutboxClaimUsesTheDatabaseLockBoundary() throws Exception {
        FindShipperEvent command = command(UUID.fromString("77777777-7777-7777-7777-777777777777"));
        String raw = objectMapper.writeValueAsString(command);
        store.acceptFindCommand("saga.command.find-shipper", raw, command);
        ShipperFoundEvent candidate = found(command, 905L);
        store.stageCandidate(command.getEventId(), candidate);
        store.stageFoundResult(command.getEventId(), candidate);

        assertThat(outboxRepository.lockNextOrderedBatch(10)).singleElement()
                .extracting(MatchOutboxEvent::getEventId)
                .isEqualTo(UUID.fromString(candidate.getEventId()));
    }

    private FindShipperEvent command(UUID eventId) {
        FindShipperEvent command = new FindShipperEvent();
        command.setEventId(eventId);
        command.setMatchingSessionId(UUID.nameUUIDFromBytes(
                ("session:" + eventId).getBytes()));
        command.setOrderId(456L);
        command.setDeliveryId(123L);
        command.setRestaurantName("Match Store Test");
        command.setPickupAddress("Pickup");
        command.setDeliveryAddress("Dropoff");
        return command;
    }

    private ShipperFoundEvent found(FindShipperEvent command, long shipperId) {
        ShipperFoundEvent result = new ShipperFoundEvent(
                command.getDeliveryId(),
                command.getOrderId(),
                List.of(new ShipperFoundEvent.ShipperMatchResult(
                        shipperId, null, null, 1.2, 10.7, 106.6, null, true)));
        result.setEventId(UUID.nameUUIDFromBytes(
                ("match:shipper-found:" + command.getEventId()).getBytes()).toString());
        result.setMatchingSessionId(command.getMatchingSessionId().toString());
        return result;
    }

    private String stopPayload(UUID stopEventId, FindShipperEvent command) throws Exception {
        var payload = objectMapper.createObjectNode();
        payload.put("eventId", stopEventId.toString());
        payload.put("orderId", command.getOrderId());
        payload.put("deliveryId", command.getDeliveryId());
        payload.put("matchingSessionId", command.getMatchingSessionId().toString());
        return objectMapper.writeValueAsString(payload);
    }
}
