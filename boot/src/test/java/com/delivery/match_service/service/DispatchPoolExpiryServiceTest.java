package com.delivery.match_service.service;

import com.delivery.match_service.MatchServiceApplication;
import com.delivery.match_service.dto.event.FindShipperEvent;
import com.delivery.match_service.entity.DispatchPoolItem;
import com.delivery.match_service.entity.MatchCommand;
import com.delivery.match_service.entity.MatchOutboxEvent;
import com.delivery.match_service.repository.DispatchPoolItemRepository;
import com.delivery.match_service.repository.DispatchRoundRepository;
import com.delivery.match_service.repository.MatchCancellationTombstoneRepository;
import com.delivery.match_service.repository.MatchCommandRepository;
import com.delivery.match_service.repository.MatchOutboxEventRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = MatchServiceApplication.class, properties = {
        "spring.kafka.listener.auto-startup=false",
        "match.kafka.listener.auto-startup=false",
        "app.outbox.relay-enabled=false",
        "matching.batch.enabled=true",
        "matching.batch.scheduler-enabled=false"
})
class DispatchPoolExpiryServiceTest {

    @Autowired
    private DispatchPoolExpiryService expiryService;

    @Autowired
    private MatchCommandStore matchCommandStore;

    @Autowired
    private DispatchPoolItemRepository poolRepository;

    @Autowired
    private DispatchRoundRepository roundRepository;

    @Autowired
    private MatchCommandRepository commandRepository;

    @Autowired
    private MatchOutboxEventRepository outboxRepository;

    @Autowired
    private MatchCancellationTombstoneRepository cancellationTombstoneRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void clean() {
        poolRepository.deleteAll();
        roundRepository.deleteAll();
        outboxRepository.deleteAll();
        commandRepository.deleteAll();
        cancellationTombstoneRepository.deleteAll();
    }

    @Test
    void expiresWaitingItemAndStagesOneStableNotFoundResultAcrossReplay() throws Exception {
        UUID commandId = UUID.fromString("11111111-1111-1111-1111-111111111111");
        UUID sessionId = UUID.fromString("22222222-2222-2222-2222-222222222222");
        FindShipperEvent command = command(commandId, sessionId);
        matchCommandStore.acceptFindCommand(
                "saga.command.find-shipper", objectMapper.writeValueAsString(command), command);

        UUID poolItemId = UUID.fromString("33333333-3333-3333-3333-333333333333");
        DispatchPoolItem item = poolItem(poolItemId, command, sessionId);
        poolRepository.saveAndFlush(item);

        assertThat(expiryService.expireDueItems()).isOne();

        DispatchPoolItem expired = poolRepository.findById(poolItemId).orElseThrow();
        assertThat(expired.getState()).isEqualTo(DispatchPoolItem.State.EXPIRED);
        assertThat(expired.getClaimedRoundId()).isNull();
        assertThat(commandRepository.findById(commandId).orElseThrow().getStatus())
                .isEqualTo(MatchCommand.Status.RESULT_STAGED);

        assertThat(outboxRepository.findAll()).singleElement().satisfies(outbox -> {
            assertThat(outbox.getTopic()).isEqualTo("shipper.not-found");
            assertThat(outbox.getEventType()).isEqualTo("shipper.not-found");
            assertThat(outbox.getCommandEventId()).isEqualTo(commandId);
            assertThat(outbox.getEventId()).isEqualTo(MatchingOutcomeEventIds
                    .forCommandOutcome("shipper-not-found", commandId));
        });
        MatchOutboxEvent result = outboxRepository.findAll().get(0);
        JsonNode payload = objectMapper.readTree(result.getPayload());
        assertThat(payload.path("orderId").asLong()).isEqualTo(command.getOrderId());
        assertThat(payload.path("deliveryId").asLong()).isEqualTo(command.getDeliveryId());
        assertThat(payload.path("matchingSessionId").asText()).isEqualTo(sessionId.toString());
        assertThat(payload.path("reason").asText())
                .isEqualTo("Matching deadline expired before a batch shipper was assigned");

        // A scheduler retry sees no WAITING row and cannot append another
        // terminal outbox event.
        assertThat(expiryService.expireDueItems()).isZero();
        assertThat(outboxRepository.count()).isOne();
    }

    private FindShipperEvent command(UUID commandId, UUID sessionId) {
        FindShipperEvent command = new FindShipperEvent();
        command.setEventId(commandId);
        command.setMatchingSessionId(sessionId);
        command.setOrderId(101L);
        command.setDeliveryId(202L);
        command.setTotalPrice(new BigDecimal("45000.00"));
        command.setPaymentMethod("COD");
        command.setPickupLat(10.7769);
        command.setPickupLng(106.7009);
        command.setDeliveryLat(10.7740);
        command.setDeliveryLng(106.7040);
        command.setRestaurantName("Sandbox Quán");
        command.setPickupAddress("123 Lê Lợi");
        command.setDeliveryAddress("456 Nguyễn Huệ");
        command.setMatchingDeadlineAt(LocalDateTime.now().minusSeconds(10));
        return command;
    }

    private DispatchPoolItem poolItem(UUID poolItemId, FindShipperEvent command, UUID sessionId) {
        LocalDateTime now = LocalDateTime.now();
        DispatchPoolItem item = new DispatchPoolItem();
        item.setPoolItemId(poolItemId);
        item.setOrderId(command.getOrderId());
        item.setDeliveryId(command.getDeliveryId());
        item.setMatchingSessionId(sessionId);
        item.setPickupLat(command.getPickupLat());
        item.setPickupLng(command.getPickupLng());
        item.setDeliveryLat(command.getDeliveryLat());
        item.setDeliveryLng(command.getDeliveryLng());
        item.setTotalPrice(command.getTotalPrice());
        item.setPaymentMethod(command.getPaymentMethod());
        item.setWaveNumber(0);
        item.setEligibleAt(now.minusMinutes(1));
        item.setMatchingDeadlineAt(now.minusSeconds(5));
        item.setState(DispatchPoolItem.State.WAITING);
        item.setVersion(0L);
        item.setCreatedAt(now.minusMinutes(1));
        item.setUpdatedAt(now.minusMinutes(1));
        return item;
    }
}
