package com.delivery.dispatch.application;

import com.delivery.dispatch.application.DefaultDeliveryProgressUseCaseTest.FakeCase;
import com.delivery.dispatch.application.api.DispatchCase;
import com.delivery.dispatch.application.api.MatchOutcomeUseCase.Outcome;
import com.delivery.dispatch.application.api.OfferCommands;
import com.delivery.dispatch.domain.DispatchStatus;
import com.delivery.dispatch.domain.OfferRetirementPolicy.Decision;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class DefaultMatchOutcomeUseCaseTest {

    private static final UUID CACHE = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID EXPIRE = UUID.fromString("22222222-2222-2222-2222-222222222222");

    private final List<String> calls = new ArrayList<>();
    private final DefaultMatchOutcomeUseCase useCase = new DefaultMatchOutcomeUseCase(
            c -> calls.add("save:" + c.status()),
            new OfferCommands() {
                @Override public void requestOfferPersistence(DispatchCase c, String found) {
                    calls.add("persist:" + found);
                    c.record("OFFER_PERSIST_REQUESTED", "cmd", "cacheCommandEventId=" + CACHE);
                }
                @Override public void markShipperNotFound(DispatchCase c, String cause) { calls.add("notFound:" + cause); }
                @Override public String startPreparedRematch(DispatchCase c, String prepared) {
                    calls.add("rematch:" + prepared);
                    return "find";
                }
            },
            (c, status, cause) -> calls.add("order:" + status + ":" + cause));

    @Test
    void foundShipperRequestsOfferPersistenceOnlyForCurrentGenerationWhileFinding() {
        FakeCase c = new FakeCase(DispatchStatus.FINDING_SHIPPER);
        assertEquals(Outcome.STALE, useCase.onShipperFound(c, () -> false, "found"));
        assertEquals(Outcome.IGNORED_STATE,
                useCase.onShipperFound(new FakeCase(DispatchStatus.SHIPPER_FOUND), () -> true, "found"));
        assertEquals(Outcome.OFFER_PERSIST_REQUESTED, useCase.onShipperFound(c, () -> true, "found"));
        assertEquals(DispatchStatus.OFFER_PERSISTING, c.status);
        assertEquals(List.of("persist:found", "save:OFFER_PERSISTING"), calls);
    }

    @Test
    void notFoundFailsTheCaseAndConvergesDeliveryAndOrder() {
        FakeCase c = new FakeCase(DispatchStatus.FINDING_SHIPPER);
        assertEquals(Outcome.STALE, useCase.onShipperNotFound(c, () -> false, "nf"));
        assertEquals(Outcome.IGNORED_STATE,
                useCase.onShipperNotFound(new FakeCase(DispatchStatus.OFFER_PERSISTING), () -> true, "nf"));
        assertEquals(Outcome.SHIPPER_NOT_FOUND, useCase.onShipperNotFound(c, () -> true, "nf"));
        assertEquals(DispatchStatus.FAILED, c.status);
        assertTrue(c.completed);
        assertEquals(List.of("save:FAILED", "notFound:nf", "order:SHIPPER_NOT_FOUND:nf"), calls);
    }

    @Test
    void offerConfirmationRequiresTheAwaitedCommandAndCurrentGeneration() {
        FakeCase c = new FakeCase(DispatchStatus.OFFER_PERSISTING,
                "OFFER_PERSIST_REQUESTED", "cacheCommandEventId=" + CACHE);
        assertEquals(Outcome.STALE, useCase.onOfferPersisted(c, UUID.randomUUID(), () -> true, "p"));
        assertEquals(Outcome.STALE, useCase.onOfferPersisted(c, CACHE, () -> false, "p"));
        assertEquals(Outcome.STALE, useCase.onOfferPersisted(
                new FakeCase(DispatchStatus.FINDING_SHIPPER), CACHE, () -> true, "p"));
        for (DispatchStatus stronger : List.of(DispatchStatus.SHIPPER_ASSIGNED, DispatchStatus.CANCELLED,
                DispatchStatus.FAILED)) {
            assertEquals(Outcome.STRONGER_STATE, useCase.onOfferPersisted(new FakeCase(stronger), CACHE,
                    () -> { throw new AssertionError("fence must not be evaluated"); }, "p"));
        }
        assertEquals(Outcome.OFFER_CONFIRMED, useCase.onOfferPersisted(c, CACHE, () -> true, "p"));
        assertEquals(DispatchStatus.SHIPPER_FOUND, c.status);
        assertEquals(List.of("save:SHIPPER_FOUND", "order:WAIT_SHIPPER_CONFIRM:p"), calls);
    }

    private static FakeCase retiring() {
        return new FakeCase(DispatchStatus.OFFER_RETIRING,
                "SHIPPER_OFFER_TIMEOUT_1", "prepared",
                "OFFER_RETIRE_REQUESTED", "expireCommandEventId=" + EXPIRE);
    }

    @Test
    void retiredOfferStartsThePreparedRematch() {
        FakeCase c = retiring();
        assertEquals(Outcome.REMATCHED, useCase.onOfferRetired(c, EXPIRE, Decision.REMATCH, null, "r"));
        assertEquals(DispatchStatus.FINDING_SHIPPER, c.status);
        assertEquals(List.of("rematch:prepared", "save:FINDING_SHIPPER", "order:FINDING_SHIPPER:find"), calls);
    }

    @Test
    void retirementReportingAcceptanceAssignsAndTerminalOnlyRecords() {
        FakeCase assigned = retiring();
        assertEquals(Outcome.ASSIGNED, useCase.onOfferRetired(assigned, EXPIRE, Decision.ASSIGN, 30L, "a"));
        assertEquals(DispatchStatus.SHIPPER_ASSIGNED, assigned.status);
        assertEquals(30L, assigned.shipperId);
        FakeCase terminal = retiring();
        assertEquals(Outcome.TERMINAL_RECORDED, useCase.onOfferRetired(terminal, EXPIRE, Decision.TERMINAL, null, "t"));
        assertEquals(DispatchStatus.OFFER_RETIRING, terminal.status);
        assertTrue(terminal.history().has("OFFER_RETIRED_TERMINAL"));
        assertEquals(List.of("save:SHIPPER_ASSIGNED", "order:SHIPPER_ASSIGNED:a", "save:OFFER_RETIRING"), calls);
    }

    @Test
    void retirementForAnotherCommandOrStateIsStaleAndMissingPreparationFails() {
        assertEquals(Outcome.STALE, useCase.onOfferRetired(retiring(), UUID.randomUUID(), Decision.REMATCH, null, "r"));
        assertEquals(Outcome.STALE, useCase.onOfferRetired(
                new FakeCase(DispatchStatus.FINDING_SHIPPER), EXPIRE, Decision.REMATCH, null, "r"));
        FakeCase unprepared = new FakeCase(DispatchStatus.OFFER_RETIRING,
                "OFFER_RETIRE_REQUESTED", "expireCommandEventId=" + EXPIRE);
        assertThrows(IllegalStateException.class,
                () -> useCase.onOfferRetired(unprepared, EXPIRE, Decision.REMATCH, null, "r"));
        assertTrue(calls.isEmpty());
    }

    @Test
    void requiresAllCollaborators() {
        OfferCommands offers = useCaseOffers();
        assertThrows(NullPointerException.class, () -> new DefaultMatchOutcomeUseCase(null, offers, (c, s, e) -> { }));
        assertThrows(NullPointerException.class, () -> new DefaultMatchOutcomeUseCase(c -> { }, null, (c, s, e) -> { }));
        assertThrows(NullPointerException.class, () -> new DefaultMatchOutcomeUseCase(c -> { }, offers, null));
    }

    private static OfferCommands useCaseOffers() {
        return new OfferCommands() {
            @Override public void requestOfferPersistence(DispatchCase c, String found) { }
            @Override public void markShipperNotFound(DispatchCase c, String cause) { }
            @Override public String startPreparedRematch(DispatchCase c, String prepared) { return ""; }
        };
    }
}
