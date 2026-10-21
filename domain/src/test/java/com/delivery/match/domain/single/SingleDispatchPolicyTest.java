package com.delivery.match.domain.single;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class SingleDispatchPolicyTest {
    private final UUID id = new UUID(0, 1);
    private FindCommand command(UUID event, Long delivery, Long order, BigDecimal money, String payment,
                                Double lat, Double lng, String name, String pickup, String dropoff) {
        return new FindCommand(event, delivery, order, money, payment, lat, lng, name, pickup, dropoff);
    }
    private FindCommand valid() { return command(id, 1L, 2L, BigDecimal.ONE, "cod", 10d, 106d, "r", "p", "d"); }
    @Test void validatesCanonicalFactsInLegacyOrder() {
        valid().validate();
        for (UUID event : Arrays.asList(null, id)) for (Long delivery : Arrays.asList(null, 0L, -1L))
            assertThrows(IllegalArgumentException.class, () -> command(event, delivery, 2L, BigDecimal.ONE, "COD", 10d, 106d, "r", "p", "d").validate());
        assertThrows(IllegalArgumentException.class, () -> command(null, 1L, 2L, BigDecimal.ONE, "COD", 10d, 106d, "r", "p", "d").validate());
        for (Long order : Arrays.asList(null, 0L, -1L))
            assertThrows(IllegalArgumentException.class, () -> command(id, 1L, order, BigDecimal.ONE, "COD", 10d, 106d, "r", "p", "d").validate());
        for (BigDecimal money : Arrays.asList(null, BigDecimal.ZERO, BigDecimal.ONE.negate()))
            assertThrows(IllegalArgumentException.class, () -> command(id, 1L, 2L, money, "COD", 10d, 106d, "r", "p", "d").validate());
        for (String payment : Arrays.asList(null, "ONLINE", " COD "))
            assertThrows(IllegalArgumentException.class, () -> command(id, 1L, 2L, BigDecimal.ONE, payment, 10d, 106d, "r", "p", "d").validate());
        for (Double lat : Arrays.asList(null, Double.NaN, Double.POSITIVE_INFINITY, 7.99, 24.01))
            assertThrows(IllegalArgumentException.class, () -> command(id, 1L, 2L, BigDecimal.ONE, "COD", lat, 106d, "r", "p", "d").validate());
        for (Double lng : Arrays.asList(null, Double.NaN, Double.NEGATIVE_INFINITY, 101.99, 110.01))
            assertThrows(IllegalArgumentException.class, () -> command(id, 1L, 2L, BigDecimal.ONE, "COD", 10d, lng, "r", "p", "d").validate());
        for (String text : Arrays.asList(null, "", " \t")) {
            assertThrows(IllegalArgumentException.class, () -> command(id, 1L, 2L, BigDecimal.ONE, "COD", 10d, 106d, text, "p", "d").validate());
            assertThrows(IllegalArgumentException.class, () -> command(id, 1L, 2L, BigDecimal.ONE, "COD", 10d, 106d, "r", text, "d").validate());
            assertThrows(IllegalArgumentException.class, () -> command(id, 1L, 2L, BigDecimal.ONE, "COD", 10d, 106d, "r", "p", text).validate());
        }
        command(id, 1L, 2L, BigDecimal.ONE, "COD", 8d, 102d, "r", "p", "d").validate();
        command(id, 1L, 2L, BigDecimal.ONE, "COD", 24d, 110d, "r", "p", "d").validate();
        assertEquals("Invalid FindShipperEvent: stable eventId and positive deliveryId are required",
                assertThrows(IllegalArgumentException.class, () -> command(null, null, null, null, null, null, null, null, null, null).validate()).getMessage());
    }
    @Test void preservesDefaultsOverridesAndUncappedFirstDelay() {
        var defaults = RetryPolicy.from(null, null, null, null);
        assertEquals(new RetryPolicy(10, 30, 300, 1.5), defaults);
        assertEquals(30000, defaults.delayMs(0));
        assertEquals(45000, defaults.delayMs(1));
        assertEquals(300000, defaults.delayMs(100));
        var overrides = RetryPolicy.from(0, 5, 2, 2d);
        assertEquals(new RetryPolicy(0, 5, 2, 2d), overrides);
        assertEquals(5000, overrides.delayMs(0));
        assertEquals(2000, overrides.delayMs(1));
        assertEquals(-1, RetryPolicy.from(-1, -2, -3, -4d).maxRetries());
    }
    @Test void absoluteWindowUsesStrictRetryCutoff() {
        var now = LocalDateTime.of(2026, 10, 4, 12, 0);
        assertFalse(SearchWindow.deadlineReached(null, now));
        assertTrue(SearchWindow.canRetry(null, now, 1000));
        assertTrue(SearchWindow.deadlineReached(now, now));
        assertTrue(SearchWindow.deadlineReached(now.minusNanos(1), now));
        assertFalse(SearchWindow.deadlineReached(now.plusNanos(1), now));
        assertFalse(SearchWindow.canRetry(now.plusSeconds(1), now, 1000));
        assertTrue(SearchWindow.canRetry(now.plusSeconds(1).plusNanos(1), now, 1000));
        assertFalse(SearchWindow.canRetry(now, now, 1));
    }
    @Test void identityAndSingleOfferAreStableAndImmutable() {
        assertEquals(id, SingleOfferPolicy.sessionId(id, null));
        UUID session = new UUID(0, 2);
        assertEquals(session, SingleOfferPolicy.sessionId(id, session));
        assertEquals(UUID.nameUUIDFromBytes(("match:shipper-found:" + id).getBytes(StandardCharsets.UTF_8)), SingleOfferPolicy.outcomeId("shipper-found", id));
        for (String outcome : Arrays.asList(null, "", " ")) assertThrows(IllegalArgumentException.class, () -> SingleOfferPolicy.outcomeId(outcome, id));
        assertThrows(IllegalArgumentException.class, () -> SingleOfferPolicy.outcomeId("found", null));
        assertEquals(180, SingleOfferPolicy.WAITING_TIMEOUT_SECONDS);
        assertEquals(5d, SingleOfferPolicy.SEARCH_RADIUS_KM);
        var first = new SingleOfferPolicy.Candidate(2L, "name", "phone", 1, 10, 106, true);
        var second = new SingleOfferPolicy.Candidate(1L, null, null, 2, 11, 107, false);
        assertEquals(List.of(first), SingleOfferPolicy.offer(List.of(first, second)));
        assertEquals(List.of(), SingleOfferPolicy.offer(List.of()));
        assertThrows(UnsupportedOperationException.class, () -> SingleOfferPolicy.offer(List.of(first)).add(second));
        assertTrue(SingleOfferPolicy.codEligible(true));
        assertFalse(SingleOfferPolicy.codEligible(false));
        assertFalse(SingleOfferPolicy.codEligible(null));
    }
    @Test void excludesWithoutReorderingOrDeduplicating() {
        var candidates = List.of(3L, 2L, 3L, 1L);
        assertEquals(List.of(0, 1, 2, 3), CandidatePolicy.exclude(new CandidatePolicy.ExclusionInput(candidates, null)).availableIndexes());
        assertFalse(CandidatePolicy.exclude(new CandidatePolicy.ExclusionInput(candidates, List.of())).exclusionApplied());
        var selected = CandidatePolicy.exclude(new CandidatePolicy.ExclusionInput(candidates, List.of(3L)));
        assertEquals(List.of(1, 3), selected.availableIndexes());
        assertEquals(List.of(3L, 3L), selected.rejectedIds());
        assertTrue(selected.exclusionApplied());
        assertEquals(CandidatePolicy.SelectionStatus.AVAILABLE, selected.status());
        assertEquals(CandidatePolicy.SelectionStatus.ALL_EXCLUDED,
                CandidatePolicy.exclude(new CandidatePolicy.ExclusionInput(candidates, List.of(1L, 2L, 3L))).status());
        assertEquals(CandidatePolicy.SelectionStatus.NO_CANDIDATES,
                CandidatePolicy.exclude(new CandidatePolicy.ExclusionInput(null, List.of(1L))).status());
        assertFalse(CandidatePolicy.exclude(new CandidatePolicy.ExclusionInput(List.of(), null)).exclusionApplied());
        var mutable = new ArrayList<Long>(candidates);
        var input = new CandidatePolicy.ExclusionInput(mutable, Arrays.asList((Long) null));
        mutable.clear();
        assertEquals(candidates, input.shipperIds());
        assertThrows(UnsupportedOperationException.class, () -> input.shipperIds().add(9L));
        assertThrows(UnsupportedOperationException.class, () -> selected.availableIndexes().add(9));
        assertThrows(UnsupportedOperationException.class, () -> selected.rejectedIds().add(9L));
        assertEquals(Collections.singletonList(null), CandidatePolicy.exclude(
                new CandidatePolicy.ExclusionInput(Collections.singletonList(null), Collections.singletonList(null))).rejectedIds());
    }

    @Test void selectsStableCanaryAndRanksRoundedScoresWithTieBreaks() {
        assertEquals(CandidatePolicy.NEAREST_COD, CandidatePolicy.select(false, 100, id));
        assertEquals(CandidatePolicy.NEAREST_COD, CandidatePolicy.select(true, -1, id));
        assertEquals(CandidatePolicy.NEAREST_COD, CandidatePolicy.select(true, 50, null));
        assertEquals(CandidatePolicy.BALANCED_ETA, CandidatePolicy.select(true, 101, id));
        assertEquals(CandidatePolicy.BALANCED_ETA, CandidatePolicy.select(true, 50, id));
        assertEquals(CandidatePolicy.NEAREST_COD, CandidatePolicy.select(true, 50, new UUID(0, 99)));
        var near = new CandidatePolicy.Candidate(14L, .1716, 20);
        var fair = new CandidatePolicy.Candidate(15L, .3272, 0);
        var ranked = CandidatePolicy.rank(CandidatePolicy.BALANCED_ETA, List.of(near, fair), .5, .03);
        assertEquals(List.of(15L, 14L), ranked.stream().map(r -> r.candidate().shipperId()).toList());
        assertEquals(.6544, ranked.get(0).score());
        assertEquals(.9432, ranked.get(1).score());
        assertNull(CandidatePolicy.rank(CandidatePolicy.NEAREST_COD, List.of(near), 0, -1).get(0).score());
        assertEquals(List.of(), CandidatePolicy.rank(CandidatePolicy.BALANCED_ETA, null, 0, -1));
        assertEquals(List.of(), CandidatePolicy.rank(CandidatePolicy.BALANCED_ETA, List.of(), 0, -1));
        for (double speed : new double[]{0, -1, Double.NaN, Double.POSITIVE_INFINITY})
            assertThrows(IllegalStateException.class, () -> CandidatePolicy.rank(CandidatePolicy.BALANCED_ETA, List.of(near), speed, 0));
        for (double penalty : new double[]{-1, Double.NaN, Double.POSITIVE_INFINITY})
            assertThrows(IllegalStateException.class, () -> CandidatePolicy.rank(CandidatePolicy.BALANCED_ETA, List.of(near), 1, penalty));
        var ties = List.of(new CandidatePolicy.Candidate(3L, 2, 0), new CandidatePolicy.Candidate(2L, 1, 1), new CandidatePolicy.Candidate(1L, 1, 1), new CandidatePolicy.Candidate(4L, 1, -1));
        assertEquals(List.of(4L, 1L, 2L, 3L), CandidatePolicy.rank(CandidatePolicy.BALANCED_ETA, ties, 1, 1).stream().map(r -> r.candidate().shipperId()).toList());
    }
}
