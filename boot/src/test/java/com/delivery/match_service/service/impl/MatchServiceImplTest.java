package com.delivery.match_service.service.impl;

import com.delivery.match_service.dto.request.FindNearbyShippersRequest;
import com.delivery.match_service.repository.MatchRedisGeoRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class MatchServiceImplTest {

    @Mock
    private MatchRedisGeoRepository repository;
    private MatchServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new MatchServiceImpl(repository);
    }

    @Test
    void propagatesGeoInfrastructureFailureInsteadOfReturningEmptyCandidates() {
        when(repository.findNearbyShippers(any(), any(), any(), any()))
                .thenThrow(new RuntimeException("redis unavailable"));

        FindNearbyShippersRequest request = new FindNearbyShippersRequest();
        request.setLatitude(10.76);
        request.setLongitude(106.66);
        request.setRadiusKm(5.0);
        request.setMaxShippers(20);

        assertThatThrownBy(() -> service.findNearbyShippers(request, 1L, "SYSTEM").block())
                .isInstanceOf(RuntimeException.class)
                .hasMessage("redis unavailable");
    }
}
