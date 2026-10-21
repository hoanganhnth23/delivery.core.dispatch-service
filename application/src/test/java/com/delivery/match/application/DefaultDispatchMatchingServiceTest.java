package com.delivery.match.application;

import com.delivery.match.domain.dispatch.DispatchBundleCandidate;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultDispatchMatchingServiceTest {
    @Test
    void selectsDisjointBoundedAssignments() {
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        UUID c = UUID.randomUUID();
        List<DispatchBundleCandidate> selected = new DefaultDispatchMatchingService().optimize(List.of(
                candidate(1L, List.of(a, b), 10),
                candidate(2L, List.of(b, c), 11),
                candidate(3L, List.of(c), 20)), 2);

        assertTrue(selected.size() <= 2);
        assertEquals(selected.stream().mapToInt(DispatchBundleCandidate::coveredOrders).sum(),
                selected.stream().flatMap(item -> item.poolItemIds().stream()).distinct().count());
    }

    private DispatchBundleCandidate candidate(Long shipper, List<UUID> items, long score) {
        return new DispatchBundleCandidate(UUID.randomUUID(), shipper, items, score, 0, score);
    }
}
