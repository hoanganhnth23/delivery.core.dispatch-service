package com.delivery.match_service.controller;

import com.delivery.match.application.api.FindNearbyShippersPort;
import com.delivery.match.application.api.FindNearbyShippersPort.FindNearbyShippersResult;
import com.delivery.match.application.api.FindNearbyShippersPort.NearbyShipper;
import com.delivery.match_service.dto.request.FindNearbyShippersRequest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MatchControllerTest {

    @Test
    void keepsNearbyShippersRouteContractAndMapsApplicationResult() {
        FindNearbyShippersPort port = mock(FindNearbyShippersPort.class);
        when(port.findNearbyShippers(any())).thenReturn(new FindNearbyShippersResult(List.of(
                new NearbyShipper(7L, 10.1, 106.2, 1.4, true, 12))));

        var response = new MatchController(port)
                .findNearbyShippers(new FindNearbyShippersRequest(10.0, 106.0, 5.0, 10), null, null)
                .block();

        assertEquals(200, response.getStatusCode().value());
        assertEquals(1, response.getBody().getStatus());
        assertEquals(1, response.getBody().getData().size());
        assertEquals(7L, response.getBody().getData().get(0).getShipperId());
        assertEquals(12L, response.getBody().getData().get(0).getCompletedDeliveries());
        verify(port).findNearbyShippers(any());
    }

    @Test
    void rejectsInvalidRequestsBeforeCallingApplicationPort() {
        FindNearbyShippersPort port = mock(FindNearbyShippersPort.class);

        var response = new MatchController(port)
                .findNearbyShippers(new FindNearbyShippersRequest(91.0, 106.0, 5.0, 10), null, null)
                .block();

        assertEquals(400, response.getStatusCode().value());
        verify(port, org.mockito.Mockito.never()).findNearbyShippers(any());
    }
}
