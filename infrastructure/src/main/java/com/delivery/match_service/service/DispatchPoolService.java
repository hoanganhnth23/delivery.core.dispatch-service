package com.delivery.match_service.service;

import com.delivery.match.domain.batch.BatchPoolPolicy;

import com.delivery.match_service.config.MatchingBatchProperties;
import com.delivery.match_service.dto.event.FindShipperEvent;
import com.delivery.match_service.entity.DispatchPoolItem;
import com.delivery.match_service.repository.DispatchPoolItemRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Durable intake boundary for rolling batch dispatch. It is intentionally
 * independent from the current per-order matcher until the scheduler and
 * optimizer are enabled together.
 */
@Service
@RequiredArgsConstructor
public class DispatchPoolService {

    private final DispatchPoolItemRepository poolRepository;
    private final MatchingBatchProperties properties;
    private final H3CellResolver h3CellResolver;

    private final Clock clock = Clock.systemDefaultZone();

    @Transactional
    public UUID enqueue(FindShipperEvent event, String pickupH3Cell) {
        if (event == null) {
            throw new IllegalArgumentException("Valid order, delivery and matching session are required");
        }
        BatchPoolPolicy.requireIntake(event.getOrderId(), event.getDeliveryId(), event.getMatchingSessionId(),
                properties.isEnabled());

        DispatchPoolItem existing = poolRepository
                .findByDeliveryAndSessionForUpdate(event.getDeliveryId(), event.getMatchingSessionId())
                .orElse(null);
        if (existing != null) {
            return existing.getPoolItemId();
        }

        LocalDateTime now = LocalDateTime.now(clock);
        LocalDateTime deadline = BatchPoolPolicy.deadline(now, event.getMatchingDeadlineAt());

        DispatchPoolItem item = new DispatchPoolItem();
        item.setPoolItemId(UUID.randomUUID());
        item.setOrderId(event.getOrderId());
        item.setDeliveryId(event.getDeliveryId());
        item.setMatchingSessionId(event.getMatchingSessionId());
        item.setPickupH3Cell(pickupH3Cell != null
                ? pickupH3Cell
                : h3CellResolver.cellFor(event.getPickupLat(), event.getPickupLng()));
        item.setPickupLat(event.getPickupLat());
        item.setPickupLng(event.getPickupLng());
        item.setDeliveryLat(event.getDeliveryLat());
        item.setDeliveryLng(event.getDeliveryLng());
        item.setTotalPrice(event.getTotalPrice());
        item.setPaymentMethod(event.getPaymentMethod());
        item.setWaveNumber(BatchPoolPolicy.initialWave(event.getBatchWave()));
        item.setEligibleAt(now);
        item.setMatchingDeadlineAt(deadline);
        item.setState(DispatchPoolItem.State.WAITING);
        item.setVersion(0L);
        item.setCreatedAt(now);
        item.setUpdatedAt(now);
        poolRepository.saveAndFlush(item);
        return item.getPoolItemId();
    }
}
