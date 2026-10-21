package com.delivery.match_service.service;

import com.delivery.identity.contracts.SimulationContext;

import java.util.UUID;

/** Canonical per-shipper delivery completion projection consumed by Match. */
public interface CompletedDeliveryProjection {
    boolean record(UUID eventId, Long shipperId, SimulationContext simulationContext);
}
