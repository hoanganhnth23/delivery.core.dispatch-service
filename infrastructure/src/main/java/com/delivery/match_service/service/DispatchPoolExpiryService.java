package com.delivery.match_service.service;

import com.delivery.match.domain.batch.BatchPoolPolicy;

import com.delivery.match_service.config.MatchingBatchProperties;
import com.delivery.match_service.dto.event.ShipperNotFoundEvent;
import com.delivery.match_service.entity.DispatchPoolItem;
import com.delivery.match_service.entity.MatchCommand;
import com.delivery.match_service.repository.DispatchPoolItemRepository;
import com.delivery.match_service.repository.MatchCommandRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Closes batch pool items whose Saga-owned matching deadline has passed.
 *
 * Ready-round queries intentionally exclude expired rows.  This sweep is the
 * complementary durable owner: it locks the waiting row, stages the terminal
 * Match result and marks the pool item expired in one database transaction.
 * The result outbox therefore survives a Match restart and exact scheduler
 * replays cannot publish a second event.
 */
@Service
@Slf4j
public class DispatchPoolExpiryService {

    private final DispatchPoolItemRepository poolRepository;
    private final MatchCommandRepository commandRepository;
    private final MatchCommandStore matchCommandStore;
    private final MatchingBatchProperties properties;
    private final int expiryBatchSize;
    private final Clock clock;

    @Autowired
    public DispatchPoolExpiryService(
            DispatchPoolItemRepository poolRepository,
            MatchCommandRepository commandRepository,
            MatchCommandStore matchCommandStore,
            MatchingBatchProperties properties) {
        this(poolRepository, commandRepository, matchCommandStore, properties,
                Clock.systemDefaultZone());
    }

    DispatchPoolExpiryService(
            DispatchPoolItemRepository poolRepository,
            MatchCommandRepository commandRepository,
            MatchCommandStore matchCommandStore,
            MatchingBatchProperties properties,
            Clock clock) {
        this.poolRepository = poolRepository;
        this.commandRepository = commandRepository;
        this.matchCommandStore = matchCommandStore;
        this.properties = properties;
        this.expiryBatchSize = Math.max(1, properties.getExpiryBatchSize());
        this.clock = clock;
    }

    /**
     * Expires one bounded page per scheduler tick.  A missing command is left
     * waiting deliberately: emitting a not-found result without the durable
     * command/generation fence would be unsafe and the next tick can recover
     * it after command persistence catches up.
     */
    @Transactional
    public int expireDueItems() {
        if (!properties.isEnabled()) {
            return 0;
        }

        LocalDateTime now = LocalDateTime.now(clock);
        List<DispatchPoolItem> items = poolRepository.findExpiredWaitingForUpdate(
                now, PageRequest.of(0, expiryBatchSize));
        int expired = 0;
        for (DispatchPoolItem item : items) {
            if (!BatchPoolPolicy.expires(BatchPoolPolicy.State.valueOf(item.getState().name()),
                    item.getMatchingDeadlineAt(), now)) {
                continue;
            }

            MatchCommand command = commandRepository.findByDeliveryAndSessionForUpdate(
                    item.getDeliveryId(), item.getMatchingSessionId()).orElse(null);
            if (command == null) {
                log.error("Cannot expire batch pool item {}: Match command is missing for delivery={} session={}",
                        item.getPoolItemId(), item.getDeliveryId(), item.getMatchingSessionId());
                continue;
            }

            ShipperNotFoundEvent outcome = notFoundOutcome(item, command, now);
            // deadlineTerminal=true intentionally wins over an unreserved
            // candidate left in MatchCommandStore by a crash/replay.
            matchCommandStore.stageNotFoundResult(command.getEventId(), outcome, true);

            item.setState(DispatchPoolItem.State.EXPIRED);
            item.setClaimedRoundId(null);
            item.setVersion(item.getVersion() + 1);
            item.setUpdatedAt(now);
            poolRepository.save(item);
            expired++;
        }
        return expired;
    }

    private ShipperNotFoundEvent notFoundOutcome(
            DispatchPoolItem item,
            MatchCommand command,
            LocalDateTime now) {
        ShipperNotFoundEvent outcome = new ShipperNotFoundEvent(
                command.getDeliveryId(), command.getOrderId(), 0);
        outcome.setEventId(MatchingOutcomeEventIds
                .forCommandOutcome("shipper-not-found", command.getEventId()).toString());
        outcome.setMatchingSessionId(command.getMatchingSessionId().toString());
        outcome.setReason(BatchPoolPolicy.EXPIRY_REASON);
        outcome.setOccurredAt(now);
        outcome.setSearchRadius(5.0d);
        outcome.setPickupLat(item.getPickupLat());
        outcome.setPickupLng(item.getPickupLng());
        outcome.setDeliveryLat(item.getDeliveryLat());
        outcome.setDeliveryLng(item.getDeliveryLng());
        return outcome;
    }

}
