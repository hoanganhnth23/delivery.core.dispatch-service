package com.delivery.match_service.service;

import com.delivery.match.domain.batch.BatchPoolPolicy;

import com.delivery.match.application.api.DispatchMatchingPort;
import com.delivery.match.domain.dispatch.DispatchBundleCandidate;
import com.delivery.match_service.dto.event.ShipperFoundEvent;
import com.delivery.match_service.entity.DispatchPoolItem;
import com.delivery.match_service.entity.DispatchRound;
import com.delivery.match_service.entity.MatchOutboxEvent;
import com.delivery.match_service.repository.DispatchPoolItemRepository;
import com.delivery.match_service.repository.DispatchRoundRepository;
import com.delivery.match_service.repository.MatchOutboxEventRepository;
import com.delivery.match_service.repository.MatchRedisGeoRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.delivery.match.domain.batch.BatchBundlePolicy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Executes a due round and commits one immutable proposal to the Match outbox. */
@Service
@RequiredArgsConstructor
@Slf4j
public class DispatchRoundExecutionService {

    private final DispatchRoundRepository roundRepository;
    private final DispatchPoolItemRepository poolRepository;
    private final com.delivery.match_service.repository.MatchCancellationTombstoneRepository cancellationTombstoneRepository;
    private final MatchRedisGeoRepository geoRepository;
    private final MatchOutboxEventRepository outboxRepository;
    private final SettlementEligibilityClient settlementEligibilityClient;
    private final com.delivery.match_service.config.MatchingBatchProperties properties;
    private final ObjectMapper objectMapper;
    private final RoutingClient routingClient;
    private final Clock clock = Clock.systemDefaultZone();
    private final DispatchMatchingPort optimizer;

    @Transactional
    public void executeDueRounds() {
        if (!properties.isEnabled() || !properties.isSchedulerEnabled()) return;
        LocalDateTime now = LocalDateTime.now(clock);
        roundRepository.findOpenDueForUpdate(now, PageRequest.of(0, 100))
                .forEach(this::execute);
    }

    @Transactional
    public void execute(DispatchRound round) {
        if (round == null || round.getState() != DispatchRound.State.OPEN) return;
        List<DispatchPoolItem> items = admissibleItems(
                poolRepository.findByClaimedRoundIdForUpdate(round.getDispatchRoundId()));
        if (items.isEmpty()) {
            close(round, DispatchRound.State.EXPIRED);
            return;
        }
        round.setState(DispatchRound.State.RUNNING);
        round.setShipperCount(0);
        round.setUpdatedAt(LocalDateTime.now(clock));
        roundRepository.save(round);

        List<DispatchBundleCandidate> candidates = candidates(items);
        List<DispatchBundleCandidate> selected = optimizer.optimize(
                candidates, Math.min(Math.min(properties.getMaxShippersPerRound(),
                        properties.getMaxShippersPerWave()), 100));
        if (selected.isEmpty()) {
            items.forEach(item -> requeue(item, now()));
            close(round, DispatchRound.State.REQUEUED);
            return;
        }

        int assigned = 0;
        Set<UUID> assignedPoolItemIds = new HashSet<>();
        for (DispatchBundleCandidate candidate : selected) {
            UUID batchId = BatchBundlePolicy.batchId(round.getDispatchRoundId(), candidate.shipperId(),
                    candidate.bundleId());
            List<DispatchPoolItem> batchItems = candidate.poolItemIds().stream()
                    .map(id -> items.stream().filter(item -> item.getPoolItemId().equals(id)).findFirst().orElse(null))
                    .filter(java.util.Objects::nonNull).toList();
            List<SettlementEligibilityClient.CodCapacityHoldRef> holds = createHolds(batchId, candidate.shipperId(), batchItems);
            if (holds == null) {
                batchItems.forEach(item -> requeue(item, now()));
                continue;
            }
            if (!geoRepository.tryReserveShipperBatchOffer(candidate.shipperId(),
                    batchItems.stream().map(DispatchPoolItem::getDeliveryId).toList(),
                    batchId, batchItems.get(0).getMatchingSessionId(), BatchBundlePolicy.OFFER_TTL_SECONDS)) {
                releaseHolds(holds);
                batchItems.forEach(item -> requeue(item, now()));
                continue;
            }
            try {
                for (ShipperFoundEvent event : toBatchEvents(batchId, candidate.shipperId(), batchItems,
                        candidate.orderedPoolItemIds(),
                        holds.stream().map(SettlementEligibilityClient.CodCapacityHoldRef::holdId).toList())) {
                    persistOutbox(batchId, round, event);
                }
            } catch (RuntimeException failure) {
                geoRepository.releaseShipperBatchOffer(candidate.shipperId(),
                        batchItems.stream().map(DispatchPoolItem::getDeliveryId).toList(),
                        batchId, batchItems.get(0).getMatchingSessionId());
                releaseHolds(holds);
                throw failure;
            }
            batchItems.forEach(item -> {
                item.setState(DispatchPoolItem.State.ASSIGNED);
                item.setWaveNumber(item.getWaveNumber() == 0 ? 1 : item.getWaveNumber());
                item.setUpdatedAt(now());
            });
            poolRepository.saveAll(batchItems);
            assignedPoolItemIds.addAll(candidate.poolItemIds());
            assigned++;
        }

        items.stream().filter(item -> !assignedPoolItemIds.contains(item.getPoolItemId())
                        && item.getState() == DispatchPoolItem.State.CLAIMED)
                .forEach(item -> requeue(item, now()));
        poolRepository.saveAll(items);
        round.setShipperCount(assigned);
        close(round, assigned > 0 ? DispatchRound.State.COMMITTED : DispatchRound.State.REQUEUED);
    }

    private List<DispatchBundleCandidate> candidates(List<DispatchPoolItem> items) {
        Map<Long, Set<UUID>> shipperOrders = new HashMap<>();
        Map<Long, MatchRedisGeoRepository.NearbyShipperResult> locations = new HashMap<>();
        Map<UUID, DispatchPoolItem> itemsById = items.stream()
                .collect(java.util.stream.Collectors.toMap(DispatchPoolItem::getPoolItemId, item -> item));
        for (DispatchPoolItem item : items) {
            if (!BatchBundlePolicy.admits(item.getWaveNumber(), properties.getMaxWaves(),
                    item.getPaymentMethod(), pickup(item), dropoff(item))) continue;
            List<MatchRedisGeoRepository.NearbyShipperResult> nearby = geoRepository.findNearbyShippers(
                    item.getPickupLat(), item.getPickupLng(), BatchBundlePolicy.SEARCH_RADIUS_KM,
                    properties.getMaxShippersPerRound());
            for (MatchRedisGeoRepository.NearbyShipperResult shipper : nearby) {
                if (!codEligible(shipper.shipperId, item.getTotalPrice())) continue;
                locations.putIfAbsent(shipper.shipperId, shipper);
                shipperOrders.computeIfAbsent(shipper.shipperId, ignored -> new HashSet<>()).add(item.getPoolItemId());
            }
        }

        List<DispatchBundleCandidate> result = new ArrayList<>();
        for (Map.Entry<Long, Set<UUID>> entry : shipperOrders.entrySet()) {
            List<UUID> orderIds = entry.getValue().stream().sorted().toList();
            MatchRedisGeoRepository.NearbyShipperResult shipper = locations.get(entry.getKey());
            BatchBundlePolicy.Point shipperPoint = new BatchBundlePolicy.Point(shipper.latitude, shipper.longitude);
            List<UUID> seedOrderIds = BatchBundlePolicy.seeds(orderIds,
                    id -> BatchBundlePolicy.distanceKm(shipperPoint, pickup(itemsById.get(id))),
                    properties.getBundleSeedOrdersPerShipper());
            int[] emitted = {0};
            BatchBundlePolicy.bundles(orderIds, seedOrderIds, combo -> {
                if (emitted[0] >= Math.max(1, properties.getMaxBundleCandidatesPerShipper())) return;
                List<DispatchPoolItem> comboItems = combo.stream()
                        .map(itemsById::get)
                        .filter(java.util.Objects::nonNull).toList();
                if (!BatchBundlePolicy.pickupsFeasible(comboItems.stream().map(this::pickup).toList())) return;
                RoutingClient.RoutePlan routePlan = routingClient.planRoute(
                        shipper.latitude, shipper.longitude, comboItems);
                long routeSeconds = routePlan.durationSeconds();
                long incremental = BatchBundlePolicy.incrementalSeconds(routeSeconds,
                        soloRouteSeconds(locations.get(entry.getKey()), comboItems));
                if (!BatchBundlePolicy.withinDetour(incremental, properties.getMaxEtaDetourSeconds())) return;
                List<UUID> orderedPoolItemIds = routePlan.orderedItems().stream()
                        .map(DispatchPoolItem::getPoolItemId).toList();
                result.add(new DispatchBundleCandidate(BatchBundlePolicy.bundleId(entry.getKey(), combo),
                        entry.getKey(), combo, orderedPoolItemIds, routeSeconds, incremental,
                        BatchBundlePolicy.score(routeSeconds, incremental)));
                emitted[0]++;
            });
        }
        return result;
    }

    private boolean codEligible(Long shipperId, BigDecimal amount) {
        if (amount == null || amount.signum() <= 0) return false;
        try {
            return Boolean.TRUE.equals(settlementEligibilityClient.isCodEligible(shipperId, amount)
                    .block(Duration.ofSeconds(2)));
        } catch (RuntimeException ex) {
            log.warn("COD eligibility unavailable for batch candidate shipper={}: {}", shipperId, ex.getMessage());
            return false;
        }
    }

    private List<SettlementEligibilityClient.CodCapacityHoldRef> createHolds(
            UUID batchId, Long shipperId, List<DispatchPoolItem> items) {
        try {
            LocalDateTime expiresAt = now().plusSeconds(BatchBundlePolicy.OFFER_TTL_SECONDS);
            List<SettlementEligibilityClient.CodCapacityHoldRequestItem> requests = items.stream()
                    .map(item -> new SettlementEligibilityClient.CodCapacityHoldRequestItem(
                            BatchBundlePolicy.codHoldId(batchId, item.getDeliveryId()),
                            BatchBundlePolicy.codOfferId(batchId, item.getDeliveryId()),
                            item.getOrderId(), item.getDeliveryId(), item.getTotalPrice(), expiresAt))
                    .toList();
            List<SettlementEligibilityClient.CodCapacityHoldRef> holds =
                    settlementEligibilityClient.createCodCapacityHolds(
                            shipperId, items.get(0).getMatchingSessionId(), batchId, batchId, requests)
                            .block(Duration.ofSeconds(2));
            if (holds == null || holds.size() != items.size()) {
                log.warn("Settlement returned incomplete COD holds for batch {}", batchId);
                return null;
            }
            return holds;
        } catch (RuntimeException failure) {
            log.warn("Unable to create COD holds for batch {}: {}", batchId, failure.getMessage());
            return null;
        }
    }

    private void releaseHolds(List<SettlementEligibilityClient.CodCapacityHoldRef> holds) {
        holds.forEach(hold -> {
            try {
                settlementEligibilityClient.transitionCodCapacityHold(hold.holdId(), "RELEASED")
                        .block(Duration.ofSeconds(2));
            } catch (RuntimeException failure) {
                log.error("COD hold release compensation failed for {}: {}", hold.holdId(), failure.getMessage());
            }
        });
    }

    private BatchBundlePolicy.Point pickup(DispatchPoolItem item) {
        return new BatchBundlePolicy.Point(item.getPickupLat(), item.getPickupLng());
    }

    private BatchBundlePolicy.Point dropoff(DispatchPoolItem item) {
        return new BatchBundlePolicy.Point(item.getDeliveryLat(), item.getDeliveryLng());
    }

    private long routeSeconds(MatchRedisGeoRepository.NearbyShipperResult shipper, List<DispatchPoolItem> items) {
        if (shipper == null) return Long.MAX_VALUE / 4;
        return routingClient.estimateRouteSeconds(shipper.latitude, shipper.longitude, items);
    }

    private long soloRouteSeconds(MatchRedisGeoRepository.NearbyShipperResult shipper, List<DispatchPoolItem> items) {
        return items.stream().mapToLong(item -> routeSeconds(shipper, List.of(item))).min().orElse(0);
    }

    private List<ShipperFoundEvent> toBatchEvents(UUID batchId, Long shipperId, List<DispatchPoolItem> items,
                                                   List<UUID> orderedPoolItemIds,
                                                   List<UUID> codHoldIds) {
        return items.stream().map(primary -> {
        ShipperFoundEvent event = new ShipperFoundEvent(primary.getDeliveryId(), primary.getOrderId(), List.of(
                new ShipperFoundEvent.ShipperMatchResult(shipperId, null, null, null, null, null, null, true)));
        event.setEventId(BatchBundlePolicy.shipperFoundEventId(batchId, primary.getDeliveryId()).toString());
        event.setMatchingSessionId(primary.getMatchingSessionId().toString());
        event.setFoundAt(LocalDateTime.now(clock));
        event.setWaitingTimeoutSeconds(BatchBundlePolicy.waitingTimeoutSeconds(properties.getWaveTimeoutSeconds()));
        event.setBatchOffer(true);
        event.setBatchId(batchId);
        event.setBatchWave(items.stream().mapToInt(DispatchPoolItem::getWaveNumber).max().orElse(0));
        event.setCodHoldIds(codHoldIds);
        Map<UUID, DispatchPoolItem> byId = items.stream()
                .collect(java.util.stream.Collectors.toMap(DispatchPoolItem::getPoolItemId, item -> item));
        List<DispatchPoolItem> orderedItemsCandidate = orderedPoolItemIds.stream()
                .map(byId::get).filter(java.util.Objects::nonNull).toList();
        if (orderedItemsCandidate.size() != items.size()) {
            orderedItemsCandidate = items.stream().sorted(Comparator.comparing(DispatchPoolItem::getOrderId,
                    Comparator.nullsLast(Long::compareTo))).toList();
        }
        final List<DispatchPoolItem> orderedItems = orderedItemsCandidate;
        int itemCount = orderedItems.size();
        event.setBatchItems(java.util.stream.IntStream.range(0, itemCount)
                .mapToObj(index -> {
                    DispatchPoolItem item = orderedItems.get(index);
                    BatchBundlePolicy.StopSequence stops = BatchBundlePolicy.stopSequence(index, itemCount);
                    return new ShipperFoundEvent.BatchItem(item.getDeliveryId(), item.getOrderId(),
                            stops.pickup(), stops.dropoff(), item.getTotalPrice(), item.getMatchingSessionId());
                }).toList());
        return event;
        }).toList();
    }

    private void persistOutbox(UUID batchId, DispatchRound round, ShipperFoundEvent event) {
        try {
            MatchOutboxEvent outbox = new MatchOutboxEvent();
            outbox.setEventId(UUID.fromString(event.getEventId()));
            outbox.setCommandEventId(batchId);
            outbox.setAggregateId("batch:" + batchId);
            outbox.setEventType("SHIPPER_FOUND_BATCH");
            outbox.setTopic("shipper.found");
            outbox.setEventKey(batchId.toString());
            outbox.setPayload(objectMapper.writeValueAsString(event));
            outbox.setStatus(MatchOutboxEvent.Status.PENDING);
            outbox.setAttempts(0);
            outbox.setNextAttemptAt(now());
            outbox.setCreatedAt(now());
            outboxRepository.save(outbox);
        } catch (Exception ex) {
            throw new IllegalStateException("Cannot serialize batch shipper proposal", ex);
        }
    }

    /**
     * Applies the generation and absolute-deadline fences before any hold or
     * reservation: a stopped generation is retired, and an item past its
     * deadline returns to WAITING so the expiry sweep stages its deterministic
     * not-found result.
     */
    private List<DispatchPoolItem> admissibleItems(List<DispatchPoolItem> claimed) {
        LocalDateTime now = now();
        List<DispatchPoolItem> admissible = new java.util.ArrayList<>();
        for (DispatchPoolItem item : claimed) {
            switch (BatchPoolPolicy.admission(
                    cancellationTombstoneRepository.existsByDeliveryIdAndMatchingSessionId(
                            item.getDeliveryId(), item.getMatchingSessionId()),
                    item.getMatchingDeadlineAt(), now)) {
                case ADMIT -> admissible.add(item);
                case CANCEL -> {
                    item.setState(DispatchPoolItem.State.CANCELLED);
                    item.setClaimedRoundId(null);
                    item.setUpdatedAt(now);
                }
                case RETURN_FOR_EXPIRY -> {
                    item.setState(DispatchPoolItem.State.WAITING);
                    item.setClaimedRoundId(null);
                    item.setUpdatedAt(now);
                }
            }
        }
        if (admissible.size() != claimed.size()) {
            poolRepository.saveAll(claimed);
        }
        return admissible;
    }

    private void requeue(DispatchPoolItem item, LocalDateTime now) {
        // REQUEUED is an audit outcome, but the same row must become eligible
        // for the next rolling round; the ready query intentionally consumes
        // WAITING only.
        item.setState(DispatchPoolItem.State.valueOf(
                BatchPoolPolicy.requeueState(item.getWaveNumber(), properties.getMaxWaves()).name()));
        item.setClaimedRoundId(null);
        item.setEligibleAt(now.plusSeconds(1));
        item.setUpdatedAt(now);
    }

    private void close(DispatchRound round, DispatchRound.State state) {
        round.setState(state);
        round.setClosedAt(now());
        round.setUpdatedAt(now());
        roundRepository.save(round);
    }

    private LocalDateTime now() { return LocalDateTime.now(clock); }
}
