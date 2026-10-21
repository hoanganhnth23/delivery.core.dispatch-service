package com.delivery.match.application;

import com.delivery.match.application.api.FindNearbyShippersPort.FindNearbyShippersQuery;
import com.delivery.match.application.api.FindNearbyShippersPort.FindNearbyShippersResult;
import com.delivery.match.application.api.FindNearbyShippersPort.NearbyShipper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DefaultFindNearbyShippersUseCaseTest {

    @Test
    void validSearchReadsTheGeoProjection() {
        List<FindNearbyShippersQuery> seen = new ArrayList<>();
        FindNearbyShippersResult stored = new FindNearbyShippersResult(
                List.of(new NearbyShipper(7L, 10.0, 106.0, 1.2, true, 3)));
        var useCase = new DefaultFindNearbyShippersUseCase(query -> { seen.add(query); return stored; });
        FindNearbyShippersQuery query = new FindNearbyShippersQuery(10.0, 106.0, 5.0, 10);

        assertSame(stored, useCase.findNearbyShippers(query));
        assertEquals(List.of(query), seen);
    }

    @Test
    void invalidSearchNeverReachesTheGeoProjection() {
        var useCase = new DefaultFindNearbyShippersUseCase(query -> fail("GEO must not be read"));
        assertEquals("Bán kính phải từ 0.1 đến 50 km", assertThrows(IllegalArgumentException.class,
                () -> useCase.findNearbyShippers(new FindNearbyShippersQuery(10, 106, 0, 10))).getMessage());
        assertEquals("Request không được null", assertThrows(IllegalArgumentException.class,
                () -> useCase.findNearbyShippers(null)).getMessage());
        assertThrows(NullPointerException.class, () -> new DefaultFindNearbyShippersUseCase(null));
    }
}
