package com.delivery.match_service.controller;

import com.delivery.match.application.api.FindNearbyShippersPort;
import com.delivery.match.domain.availability.NearbySearchPolicy;
import com.delivery.match.application.api.FindNearbyShippersPort.FindNearbyShippersQuery;
import com.delivery.match.application.api.FindNearbyShippersPort.NearbyShipper;
import com.delivery.match_service.common.constants.ApiPathConstants;
import com.delivery.match_service.common.constants.HttpHeaderConstants;
import com.delivery.match_service.dto.request.FindNearbyShippersRequest;
import com.delivery.match_service.dto.response.NearbyShipperResponse;
import com.delivery.match_service.payload.BaseResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;

@RestController
@RequestMapping(ApiPathConstants.MATCH)
public class MatchController {

    private final FindNearbyShippersPort findNearbyShippers;

    public MatchController(FindNearbyShippersPort findNearbyShippers) {
        this.findNearbyShippers = findNearbyShippers;
    }

    @PostMapping(value = ApiPathConstants.NEARBY_SHIPPERS,
            consumes = {"application/json", "text/plain", "*/*"}, produces = "application/json")
    public Mono<ResponseEntity<BaseResponse<List<NearbyShipperResponse>>>> findNearbyShippers(
            @RequestBody FindNearbyShippersRequest request,
            @RequestHeader(value = HttpHeaderConstants.X_USER_ID, required = false) Long userId,
            @RequestHeader(value = HttpHeaderConstants.X_ROLE, required = false) String role) {
        String validationError = request == null
                ? "Request không được null"
                : NearbySearchPolicy.validationError(request.getLatitude(), request.getLongitude(),
                        request.getRadiusKm(), request.getMaxShippers());
        if (validationError != null) {
            return Mono.just(ResponseEntity.badRequest().body(new BaseResponse<>(0, null, validationError)));
        }

        FindNearbyShippersQuery query = new FindNearbyShippersQuery(
                request.getLatitude(), request.getLongitude(), request.getRadiusKm(), request.getMaxShippers());
        return Mono.fromCallable(() -> findNearbyShippers.findNearbyShippers(query))
                .subscribeOn(Schedulers.boundedElastic())
                .map(result -> ResponseEntity.ok(new BaseResponse<>(1, toResponses(result.shippers()),
                        "Tìm thấy " + result.shippers().size() + " shipper gần vị trí yêu cầu")))
                .onErrorReturn(ResponseEntity.status(500)
                        .body(new BaseResponse<>(0, null, "Lỗi khi tìm kiếm shipper gần nhất")));
    }

    private List<NearbyShipperResponse> toResponses(List<NearbyShipper> shippers) {
        return shippers.stream().map(shipper -> {
            NearbyShipperResponse response = new NearbyShipperResponse(shipper.shipperId(), null, null,
                    shipper.latitude(), shipper.longitude(), shipper.distanceKm(), shipper.online(), null);
            response.setCompletedDeliveries(shipper.completedDeliveries());
            return response;
        }).toList();
    }
}
