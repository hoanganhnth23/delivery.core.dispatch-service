package com.delivery.match_service.service;

import com.delivery.match_service.dto.request.FindNearbyShippersRequest;
import com.delivery.match_service.dto.response.NearbyShipperResponse;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;
import com.delivery.identity.contracts.SimulationContext;
import com.delivery.match.application.api.FindNearbyShippersPort.FindNearbyShippersQuery;
import com.delivery.match.application.api.FindNearbyShippersPort.FindNearbyShippersResult;

/**
 * ✅ Interface cho Match Service với Non-blocking approach
 * Theo Backend Instructions: Service interface pattern
 */
public interface MatchService {

    FindNearbyShippersResult findNearbyShippers(FindNearbyShippersQuery query);

    /**
     * Tìm các shipper gần nhất từ Tracking Service (Non-blocking)
     * 
     * @param request Thông tin vị trí và bán kính tìm kiếm
     * @return Mono<List> các shipper gần nhất
     */
    Mono<List<NearbyShipperResponse>> findNearbyShippers(FindNearbyShippersRequest request, Long userId, String role);

    Mono<List<NearbyShipperResponse>> findNearbyShippers(FindNearbyShippersRequest request, Long userId, String role,
                                                         SimulationContext simulationContext);

    /** Atomically reserve a shipper for one outstanding offer. */
    boolean tryReserveShipperOffer(Long shipperId, Long deliveryId, UUID matchingSessionId, int timeoutSeconds);
    boolean tryReserveShipperOffer(Long shipperId, Long deliveryId, UUID matchingSessionId, int timeoutSeconds,
                                   SimulationContext simulationContext);

    /** Release only the offer still owned by this shipper and delivery pair. */
    boolean releaseShipperOffer(Long shipperId, Long deliveryId, UUID matchingSessionId);
    boolean releaseShipperOffer(Long shipperId, Long deliveryId, UUID matchingSessionId,
                                SimulationContext simulationContext);
    
    /**
     * Dừng quá trình matching cho một delivery cụ thể
     * 
     * @param deliveryId ID của delivery bị hủy
     * @param orderId ID của order bị hủy
     * @param reason Lý do hủy
     */
}
