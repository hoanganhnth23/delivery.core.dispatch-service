package com.delivery.match.application;

import com.delivery.match.domain.dispatch.DispatchBundleCandidate;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DispatchMatchingCoverageTest {
    @Test
    void handlesEmptyInputsAndAssignmentLimit() {
        var service = new DefaultDispatchMatchingService();
        assertTrue(service.optimize(null, 2).isEmpty());
        assertTrue(service.optimize(List.of(), 2).isEmpty());
        assertTrue(service.optimize(List.of(candidate(1, List.of(UUID.randomUUID()), 1)), 0).isEmpty());
    }

    @Test
    void filtersInvalidAdapterCandidateAndStopsWhenNoDisjointCandidateRemains() {
        DispatchBundleCandidate invalid = mock(DispatchBundleCandidate.class);
        when(invalid.poolItemIds()).thenReturn(List.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()));
        DispatchBundleCandidate only = candidate(1, List.of(UUID.randomUUID()), 1);
        var selected = new DefaultDispatchMatchingService().optimize(List.of(invalid, only), 5);
        assertEquals(1, selected.size());
    }

    @Test
    void keepsDeterministicQualityOrdering() {
        UUID order = UUID.randomUUID();
        DispatchBundleCandidate slow = new DispatchBundleCandidate(UUID.randomUUID(), 1L, List.of(order), 10, 0, 20);
        DispatchBundleCandidate fast = new DispatchBundleCandidate(UUID.randomUUID(), 2L, List.of(UUID.randomUUID()), 5, 0, 10);
        var selected = new DefaultDispatchMatchingService().optimize(List.of(slow, fast), 2);
        assertEquals(2, selected.size());
        assertEquals(2L, selected.get(0).shipperId());
    }

    private DispatchBundleCandidate candidate(long shipper, List<UUID> orders, long score) {
        return new DispatchBundleCandidate(UUID.randomUUID(), shipper, orders, score, 0, score);
    }
}
