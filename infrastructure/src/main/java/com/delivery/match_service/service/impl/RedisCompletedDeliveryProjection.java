package com.delivery.match_service.service.impl;

import com.delivery.identity.contracts.SimulationContext;
import com.delivery.match_service.repository.MatchRedisGeoRepository;
import com.delivery.match_service.service.CompletedDeliveryProjection;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class RedisCompletedDeliveryProjection implements CompletedDeliveryProjection {
    private final MatchRedisGeoRepository repository;

    public RedisCompletedDeliveryProjection(MatchRedisGeoRepository repository) {
        this.repository = repository;
    }

    @Override
    public boolean record(UUID eventId, Long shipperId, SimulationContext simulationContext) {
        return repository.recordCompletedDelivery(eventId, shipperId, simulationContext);
    }
}
