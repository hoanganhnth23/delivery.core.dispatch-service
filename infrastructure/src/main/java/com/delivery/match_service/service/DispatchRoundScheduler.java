package com.delivery.match_service.service;

import com.delivery.match_service.config.MatchingBatchProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.LocalDateTime;

/**
 * Opens zone-local rounds. Optimizer execution is intentionally a later step;
 * the scheduler is separately gated so enabling the pool cannot strand orders.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DispatchRoundScheduler {

    private final MatchingBatchProperties properties;
    private final com.delivery.match_service.repository.DispatchPoolItemRepository poolRepository;
    private final DispatchRoundService roundService;
    private final DispatchRoundExecutionService executionService;
    private final DispatchPoolExpiryService expiryService;
    private final Clock clock = Clock.systemDefaultZone();

    @Scheduled(fixedDelayString = "${matching.batch.scheduler-delay-ms:1000}")
    public void openReadyRounds() {
        if (!properties.isEnabled() || !properties.isSchedulerEnabled()) {
            return;
        }

        // Ready queries deliberately skip rows past the absolute cutoff. Sweep
        // them first so the batch path cannot strand an order in FINDING_SHIPPER.
        expiryService.expireDueItems();
        executionService.executeDueRounds();
        LocalDateTime now = LocalDateTime.now(clock);
        poolRepository.findReadyZones(now, PageRequest.of(0, 100))
                .forEach(zone -> roundService.openAndClaim(zone)
                        .ifPresent(round -> log.info(
                                "Claimed dispatch round {} zone={} orders={} cutoff={}",
                                round.round().getDispatchRoundId(),
                                zone,
                                round.items().size(),
                                round.round().getCutoffAt())));
    }
}
