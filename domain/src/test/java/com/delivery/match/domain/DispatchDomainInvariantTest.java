package com.delivery.match.domain;

import com.delivery.match.domain.dispatch.DispatchBundleCandidate;
import com.delivery.match.domain.dispatch.DispatchOrderSnapshot;
import com.delivery.match.domain.dispatch.DispatchShipperSnapshot;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DispatchDomainInvariantTest {
    @Test
    void candidateRequiresBoundedUniqueRouteAndImmutableLists() {
        UUID bundle = UUID.randomUUID(); UUID first = UUID.randomUUID(); UUID second = UUID.randomUUID();
        var candidate = new DispatchBundleCandidate(bundle, 7L, List.of(first, second), List.of(second, first), 10, 2, 100);
        assertEquals(List.of(first, second), candidate.poolItemIds());
        assertEquals(2, candidate.coveredOrders());
        assertEquals(1, new DispatchBundleCandidate(bundle, 7L, List.of(first), 1, 0, 1).coveredOrders());
        assertThrows(IllegalArgumentException.class, () -> new DispatchBundleCandidate(null, 7L, List.of(first), 1, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new DispatchBundleCandidate(bundle, null, List.of(first), 1, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new DispatchBundleCandidate(bundle, 7L, null, 1, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new DispatchBundleCandidate(bundle, 7L, List.of(), 1, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new DispatchBundleCandidate(bundle, 7L, List.of(first, second), List.of(first), 1, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new DispatchBundleCandidate(bundle, 7L, List.of(first, second), List.of(first, first), 1, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new DispatchBundleCandidate(bundle, 7L, List.of(first, second, UUID.randomUUID(), UUID.randomUUID()), 1, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new DispatchBundleCandidate(bundle, 7L, List.of(first), -1, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new DispatchBundleCandidate(bundle, 7L, List.of(first), 1, -1, 1));
        assertThrows(IllegalArgumentException.class, () -> new DispatchBundleCandidate(bundle, 7L, List.of(first), 1, 0, -1));
    }

    @Test
    void snapshotRecordsRemainDataOnly() {
        var order = new DispatchOrderSnapshot(UUID.randomUUID(), 1L, 2L, 21.0, 105.0, 21.1, 105.1, LocalDateTime.MIN);
        var shipper = new DispatchShipperSnapshot(7L, 21.0, 105.0, true, true, false, true,
                BigDecimal.valueOf(100), 30, 2);
        assertEquals(1L, order.orderId());
        assertEquals(7L, shipper.shipperId());
    }
}
