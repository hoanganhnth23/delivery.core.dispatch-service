package com.delivery.dispatch.application.api;

import com.delivery.dispatch.domain.OfferRetirementPolicy;

import java.util.UUID;
import java.util.function.BooleanSupplier;

/**
 * Match results and Delivery offer acknowledgements. {@code currentGeneration}
 * is evaluated lazily, exactly where the generation fence applies, and may
 * throw for a generation-aware case whose result lacks a valid session.
 */
public interface MatchOutcomeUseCase {

    enum Outcome {
        STALE, IGNORED_STATE, STRONGER_STATE,
        OFFER_PERSIST_REQUESTED, SHIPPER_NOT_FOUND, OFFER_CONFIRMED,
        REMATCHED, ASSIGNED, TERMINAL_RECORDED
    }

    Outcome onShipperFound(DispatchCase dispatchCase, BooleanSupplier currentGeneration, String rawEvent);

    Outcome onShipperNotFound(DispatchCase dispatchCase, BooleanSupplier currentGeneration, String rawEvent);

    Outcome onOfferPersisted(DispatchCase dispatchCase, UUID sourceCommandId, BooleanSupplier currentGeneration,
                             String rawEvent);

    Outcome onOfferRetired(DispatchCase dispatchCase, UUID sourceCommandId, OfferRetirementPolicy.Decision decision,
                           Long shipperId, String rawEvent);
}
