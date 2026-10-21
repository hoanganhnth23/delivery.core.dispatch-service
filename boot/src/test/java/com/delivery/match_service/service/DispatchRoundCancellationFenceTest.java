package com.delivery.match_service.service;

import com.delivery.match_service.MatchServiceApplication;
import com.delivery.match_service.entity.DispatchPoolItem;
import com.delivery.match_service.entity.DispatchRound;
import com.delivery.match_service.entity.MatchCancellationTombstone;
import com.delivery.match_service.repository.DispatchPoolItemRepository;
import com.delivery.match_service.repository.DispatchRoundRepository;
import com.delivery.match_service.repository.MatchCancellationTombstoneRepository;
import com.delivery.match_service.repository.MatchCommandRepository;
import com.delivery.match_service.repository.MatchOutboxEventRepository;
import com.delivery.match_service.repository.MatchRedisGeoRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Batch dispatch must honor the same generation cancellation and absolute
 * matching deadline as single dispatch (delivery-matching.md generation and
 * deadline rules): a stopped generation leaves the pool, and a claimed item
 * past its deadline is never offered.
 */
@SpringBootTest(classes = MatchServiceApplication.class, properties = {
        "spring.kafka.listener.auto-startup=false",
        "match.kafka.listener.auto-startup=false",
        "app.outbox.relay-enabled=false",
        "matching.batch.enabled=true",
        "matching.batch.scheduler-enabled=false"
})
class DispatchRoundCancellationFenceTest {

    @Autowired DispatchRoundExecutionService executionService;
    @Autowired MatchCommandStore commandStore;
    @Autowired DispatchPoolItemRepository poolRepository;
    @Autowired DispatchRoundRepository roundRepository;
    @Autowired MatchCancellationTombstoneRepository tombstoneRepository;
    @Autowired MatchCommandRepository commandRepository;
    @Autowired MatchOutboxEventRepository outboxRepository;
    @MockBean MatchRedisGeoRepository geoRepository;
    @MockBean SettlementEligibilityClient settlementEligibilityClient;

    @BeforeEach
    void clean() {
        poolRepository.deleteAll();
        roundRepository.deleteAll();
        outboxRepository.deleteAll();
        commandRepository.deleteAll();
        tombstoneRepository.deleteAll();
    }

    @Test
    void stopMatchingRetiresWaitingPoolItemOfThatGeneration() {
        UUID session = UUID.randomUUID();
        DispatchPoolItem item = item(701L, session, DispatchPoolItem.State.WAITING, null,
                LocalDateTime.now().plusMinutes(5));
        UUID otherSession = UUID.randomUUID();
        DispatchPoolItem rematch = item(701L, otherSession, DispatchPoolItem.State.WAITING, null,
                LocalDateTime.now().plusMinutes(5));

        commandStore.recordStopMatching(UUID.randomUUID(), 801L, 701L, session,
                "{\"orderId\":801,\"deliveryId\":701,\"matchingSessionId\":\"" + session + "\"}");

        assertThat(poolRepository.findById(item.getPoolItemId()).orElseThrow().getState())
                .isEqualTo(DispatchPoolItem.State.CANCELLED);
        assertThat(poolRepository.findById(rematch.getPoolItemId()).orElseThrow().getState())
                .isEqualTo(DispatchPoolItem.State.WAITING);
    }

    @Test
    void roundExcludesCancelledAndExpiredItemsBeforeAnyReservation() {
        LocalDateTime now = LocalDateTime.now();
        DispatchRound round = round(now);
        UUID cancelledSession = UUID.randomUUID();
        DispatchPoolItem cancelled = item(702L, cancelledSession, DispatchPoolItem.State.CLAIMED,
                round.getDispatchRoundId(), now.plusMinutes(5));
        tombstoneRepository.saveAndFlush(new MatchCancellationTombstone(
                UUID.randomUUID(), 802L, 702L, cancelledSession, "f".repeat(64)));
        DispatchPoolItem expired = item(703L, UUID.randomUUID(), DispatchPoolItem.State.CLAIMED,
                round.getDispatchRoundId(), now.minusSeconds(1));

        executionService.execute(roundRepository.findById(round.getDispatchRoundId()).orElseThrow());

        assertThat(poolRepository.findById(cancelled.getPoolItemId()).orElseThrow().getState())
                .isEqualTo(DispatchPoolItem.State.CANCELLED);
        DispatchPoolItem expiredAfter = poolRepository.findById(expired.getPoolItemId()).orElseThrow();
        // Left WAITING past its deadline so the expiry sweep stages the deterministic not-found.
        assertThat(expiredAfter.getState()).isEqualTo(DispatchPoolItem.State.WAITING);
        assertThat(expiredAfter.getClaimedRoundId()).isNull();
        verifyNoInteractions(geoRepository, settlementEligibilityClient);
        assertThat(roundRepository.findById(round.getDispatchRoundId()).orElseThrow().getState())
                .isEqualTo(DispatchRound.State.EXPIRED);
    }

    private DispatchRound round(LocalDateTime now) {
        DispatchRound round = new DispatchRound();
        round.setDispatchRoundId(UUID.randomUUID());
        round.setH3Zone("8865b6b6c9fffff");
        round.setState(DispatchRound.State.OPEN);
        round.setOpenedAt(now.minusSeconds(10));
        round.setCutoffAt(now.minusSeconds(5));
        round.setOrderCount(2);
        round.setShipperCount(0);
        round.setCreatedAt(now.minusSeconds(10));
        round.setUpdatedAt(now.minusSeconds(10));
        return roundRepository.saveAndFlush(round);
    }

    private DispatchPoolItem item(long deliveryId, UUID session, DispatchPoolItem.State state,
                                  UUID roundId, LocalDateTime deadline) {
        LocalDateTime now = LocalDateTime.now();
        DispatchPoolItem item = new DispatchPoolItem();
        item.setPoolItemId(UUID.randomUUID());
        item.setOrderId(deliveryId + 100);
        item.setDeliveryId(deliveryId);
        item.setMatchingSessionId(session);
        item.setPickupH3Cell("8865b6b6c9fffff");
        item.setPickupLat(10.77);
        item.setPickupLng(106.70);
        item.setDeliveryLat(10.78);
        item.setDeliveryLng(106.71);
        item.setTotalPrice(new BigDecimal("100000"));
        item.setPaymentMethod("COD");
        item.setWaveNumber(0);
        item.setEligibleAt(now.minusSeconds(10));
        item.setMatchingDeadlineAt(deadline);
        item.setState(state);
        item.setClaimedRoundId(roundId);
        item.setCreatedAt(now);
        item.setUpdatedAt(now);
        return poolRepository.saveAndFlush(item);
    }
}
