package com.delivery.match_service.service;

import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public interface SettlementEligibilityClient {
    Mono<Boolean> isCodEligible(Long shipperId, BigDecimal codAmount);

    /** Creates one hold per order, atomically at Settlement, for a proposed batch. */
    default Mono<List<CodCapacityHoldRef>> createCodCapacityHolds(
            Long shipperId, UUID matchingSessionId, UUID waveId, UUID eventId,
            List<CodCapacityHoldRequestItem> offers) {
        return Mono.error(new UnsupportedOperationException("COD batch holds are not configured"));
    }

    default Mono<Boolean> transitionCodCapacityHold(UUID holdId, String target) {
        return Mono.error(new UnsupportedOperationException("COD batch hold transitions are not configured"));
    }

    record CodCapacityHoldRequestItem(UUID holdId, UUID offerId, Long orderId, Long deliveryId,
                                      BigDecimal amount, LocalDateTime expiresAt) {
    }

    record CodCapacityHoldRef(UUID holdId, UUID offerId, Long orderId, Long deliveryId) {
    }
}
