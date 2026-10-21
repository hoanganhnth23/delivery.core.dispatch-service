package com.delivery.match_service.service;

import com.delivery.match_service.config.MatchingBatchProperties;
import com.delivery.match_service.entity.DispatchPoolItem;
import com.delivery.match_service.entity.DispatchRound;
import com.delivery.match_service.repository.DispatchPoolItemRepository;
import com.delivery.match_service.repository.DispatchRoundRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Claims a bounded, zone-local dispatch round without touching volatile Redis. */
@Service
@RequiredArgsConstructor
public class DispatchRoundService {

    private final DispatchPoolItemRepository poolRepository;
    private final DispatchRoundRepository roundRepository;
    private final MatchingBatchProperties properties;
    private final H3CellResolver h3CellResolver;
    private final Clock clock = Clock.systemDefaultZone();

    public record ClaimedRound(DispatchRound round, List<DispatchPoolItem> items) {
    }

    @Transactional
    public Optional<ClaimedRound> openAndClaim(String zone) {
        if (zone == null || zone.isBlank()) {
            throw new IllegalArgumentException("Dispatch zone is required");
        }
        if (!properties.isEnabled()) {
            return Optional.empty();
        }

        Optional<DispatchRound> existing = roundRepository
                .findFirstByH3ZoneAndStateOrderByOpenedAtAsc(zone, DispatchRound.State.OPEN);
        if (existing.isPresent()) {
            return Optional.empty();
        }

        LocalDateTime now = LocalDateTime.now(clock);
        List<String> zones = h3CellResolver.kRing(zone, properties.getNeighborRing());
        List<DispatchPoolItem> items = zones.size() <= 1
                ? poolRepository.findReadyByZoneForUpdate(zone, now,
                PageRequest.of(0, Math.max(1, Math.min(properties.getMaxOrdersPerRound(), 500))))
                : poolRepository.findReadyByZonesForUpdate(zones, now,
                PageRequest.of(0, Math.max(1, Math.min(properties.getMaxOrdersPerRound(), 500))));
        if (items.isEmpty()) {
            return Optional.empty();
        }

        DispatchRound round = new DispatchRound();
        round.setDispatchRoundId(UUID.randomUUID());
        round.setH3Zone(zone);
        round.setState(DispatchRound.State.OPEN);
        round.setOpenedAt(now);
        round.setCutoffAt(now.plusSeconds(Math.max(1, properties.getWindowSeconds())));
        round.setOrderCount(items.size());
        round.setShipperCount(0);
        round.setCreatedAt(now);
        round.setUpdatedAt(now);
        roundRepository.save(round);

        for (DispatchPoolItem item : items) {
            item.setState(DispatchPoolItem.State.CLAIMED);
            item.setClaimedRoundId(round.getDispatchRoundId());
            item.setVersion(item.getVersion() + 1);
            item.setUpdatedAt(now);
        }
        poolRepository.saveAll(items);
        return Optional.of(new ClaimedRound(round, items));
    }
}
