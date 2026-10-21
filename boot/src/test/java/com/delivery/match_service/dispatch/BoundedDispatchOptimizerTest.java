package com.delivery.match_service.dispatch;

import com.delivery.match.domain.dispatch.DispatchBundleCandidate;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BoundedDispatchOptimizerTest {
    @Test
    void selectsDisjointBundlesAndKeepsBundleBounded() {
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        UUID c = UUID.randomUUID();
        List<DispatchBundleCandidate> selected = new BoundedDispatchOptimizer().optimize(List.of(
                candidate(1L, List.of(a, b), 10),
                candidate(2L, List.of(b, c), 11),
                candidate(3L, List.of(c), 20)), 2);

        assertTrue(selected.size() <= 2);
        assertTrue(selected.stream().allMatch(item -> item.poolItemIds().size() <= 3));
        long covered = selected.stream().flatMap(item -> item.poolItemIds().stream()).distinct().count();
        long total = selected.stream().mapToLong(item -> item.poolItemIds().size()).sum();
        assertEquals(total, covered);
    }

    private DispatchBundleCandidate candidate(Long shipper, List<UUID> items, long score) {
        return new DispatchBundleCandidate(UUID.randomUUID(), shipper, items,
                score, 0, score);
    }
}
