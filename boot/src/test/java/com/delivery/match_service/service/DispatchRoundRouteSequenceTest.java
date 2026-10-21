package com.delivery.match_service.service;

import com.delivery.match_service.MatchServiceApplication;
import com.delivery.match_service.dto.event.ShipperFoundEvent;
import com.delivery.match_service.entity.DispatchPoolItem;
import com.delivery.match_service.entity.DispatchRound;
import com.delivery.match_service.repository.DispatchPoolItemRepository;
import com.delivery.match_service.repository.DispatchRoundRepository;
import com.delivery.match_service.repository.MatchOutboxEventRepository;
import com.delivery.match_service.repository.MatchRedisGeoRepository;
import com.delivery.routing.contracts.Coordinate;
import com.delivery.routing.contracts.RouteRequest;
import com.delivery.routing.contracts.RouteResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

/** Exercises real route scoring, selection and serialized durable proposals together. */
@SpringBootTest(classes = MatchServiceApplication.class, properties = {
        "spring.kafka.listener.auto-startup=false",
        "match.kafka.listener.auto-startup=false",
        "app.outbox.relay-enabled=false",
        "matching.batch.enabled=true",
        "matching.batch.scheduler-enabled=false"
})
class DispatchRoundRouteSequenceTest {
    @Autowired DispatchRoundExecutionService executionService;
    @Autowired RoutingClient routingClient;
    @Autowired DispatchPoolItemRepository poolRepository;
    @Autowired DispatchRoundRepository roundRepository;
    @Autowired MatchOutboxEventRepository outboxRepository;
    @Autowired ObjectMapper objectMapper;
    @MockBean MatchRedisGeoRepository geoRepository;
    @MockBean SettlementEligibilityClient settlementEligibilityClient;
    @MockBean com.delivery.routing.client.RoutingClient platformRoutingClient;

    @BeforeEach
    void clean() {
        poolRepository.deleteAll();
        roundRepository.deleteAll();
        outboxRepository.deleteAll();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void persistsExactlyTheScoredInterleavedRouteIncludingSingleOrder(int size) throws Exception {
        LocalDateTime now = LocalDateTime.now();
        DispatchRound round = new DispatchRound();
        round.setDispatchRoundId(UUID.randomUUID());
        round.setH3Zone("8865b6b6c9fffff");
        round.setState(DispatchRound.State.OPEN);
        round.setOpenedAt(now.minusSeconds(10));
        round.setCutoffAt(now.minusSeconds(5));
        round.setOrderCount(size);
        round.setShipperCount(0);
        round.setCreatedAt(now);
        round.setUpdatedAt(now);
        roundRepository.saveAndFlush(round);

        // Distinct coordinates per invocation avoid the routing client's short-lived leg cache.
        double latitude = 10.77 + size * 0.01;
        Coordinate origin = new Coordinate(latitude, 106.70);
        List<DispatchPoolItem> items = IntStream.rangeClosed(1, size)
                .mapToObj(index -> item(index, latitude, round.getDispatchRoundId(), now)).toList();
        poolRepository.saveAllAndFlush(items);
        List<DispatchPoolItem> expectedOrder = new ArrayList<>(items);
        java.util.Collections.reverse(expectedOrder);
        List<Coordinate> scoredStops = expectedOrder.stream().flatMap(item -> java.util.stream.Stream.of(
                new Coordinate(item.getPickupLat(), item.getPickupLng()),
                new Coordinate(item.getDeliveryLat(), item.getDeliveryLng()))).toList();
        List<RouteRequest> scoredLegs = IntStream.range(0, scoredStops.size())
                .mapToObj(index -> new RouteRequest("driving-traffic",
                        index == 0 ? origin : scoredStops.get(index - 1), scoredStops.get(index), null, false))
                .toList();
        when(platformRoutingClient.getRoute(any(RouteRequest.class))).thenAnswer(call ->
                new RouteResponse(scoredLegs.contains(call.getArgument(0)) ? 1 : 100, 10, null, "TEST"));
        when(geoRepository.findNearbyShippers(anyDouble(), anyDouble(), anyDouble(), anyInt()))
                .thenReturn(List.of(new MatchRedisGeoRepository.NearbyShipperResult(
                        7L, origin.lat(), origin.lng(), 0.0)));
        when(settlementEligibilityClient.isCodEligible(eq(7L), any())).thenReturn(Mono.just(true));
        when(settlementEligibilityClient.createCodCapacityHolds(eq(7L), any(), any(), any(), anyList()))
                .thenAnswer(call -> {
                    List<SettlementEligibilityClient.CodCapacityHoldRequestItem> requests = call.getArgument(4);
                    return Mono.just(requests.stream().map(request ->
                            new SettlementEligibilityClient.CodCapacityHoldRef(request.holdId(), request.offerId(),
                                    request.orderId(), request.deliveryId())).toList());
                });
        when(geoRepository.tryReserveShipperBatchOffer(eq(7L), anyList(), any(), any(), anyInt()))
                .thenReturn(true);

        RoutingClient.RoutePlan scored = routingClient.planRoute(origin.lat(), origin.lng(), items);
        assertThat(scored.orderedItems()).containsExactlyElementsOf(expectedOrder);
        assertThat(scored.durationSeconds()).isEqualTo(2L * size);

        executionService.execute(round);

        assertThat(roundRepository.findById(round.getDispatchRoundId()).orElseThrow().getState())
                .isEqualTo(DispatchRound.State.COMMITTED);
        var outbox = outboxRepository.findAll();
        assertThat(outbox).hasSize(size);
        for (var row : outbox) {
            assertThat(row.getTopic()).isEqualTo("shipper.found");
            assertThat(row.getEventType()).isEqualTo("SHIPPER_FOUND_BATCH");
            ShipperFoundEvent event = objectMapper.readValue(row.getPayload(), ShipperFoundEvent.class);
            assertThat(event.getBatchItems()).extracting(ShipperFoundEvent.BatchItem::getOrderId)
                    .containsExactlyElementsOf(scored.orderedItems().stream().map(DispatchPoolItem::getOrderId).toList());
            List<Stop> emitted = new ArrayList<>();
            for (ShipperFoundEvent.BatchItem batchItem : event.getBatchItems()) {
                assertThat(batchItem.getPickupSequence()).isLessThan(batchItem.getDropoffSequence());
                DispatchPoolItem item = items.stream().filter(value -> value.getOrderId().equals(batchItem.getOrderId()))
                        .findFirst().orElseThrow();
                emitted.add(new Stop(batchItem.getPickupSequence(), new Coordinate(item.getPickupLat(), item.getPickupLng())));
                emitted.add(new Stop(batchItem.getDropoffSequence(), new Coordinate(item.getDeliveryLat(), item.getDeliveryLng())));
            }
            emitted.sort(Comparator.comparingInt(Stop::sequence));
            assertThat(emitted).extracting(Stop::sequence)
                    .containsExactlyElementsOf(IntStream.range(0, 2 * size).boxed().toList());
            assertThat(emitted).extracting(Stop::coordinate).containsExactlyElementsOf(scoredStops);
            if (size == 1) {
                assertThat(event.getBatchItems().get(0).getPickupSequence()).isZero();
                assertThat(event.getBatchItems().get(0).getDropoffSequence()).isEqualTo(1);
            }
        }
    }

    private DispatchPoolItem item(int index, double latitude, UUID roundId, LocalDateTime now) {
        DispatchPoolItem item = new DispatchPoolItem();
        item.setPoolItemId(new UUID(0, index));
        item.setOrderId(800L + index);
        item.setDeliveryId(700L + index);
        item.setMatchingSessionId(UUID.randomUUID());
        item.setPickupH3Cell("8865b6b6c9fffff");
        item.setPickupLat(latitude + index * 0.001);
        item.setPickupLng(106.70);
        item.setDeliveryLat(latitude + index * 0.001 + 0.0005);
        item.setDeliveryLng(106.70);
        item.setTotalPrice(new BigDecimal("100000"));
        item.setPaymentMethod("COD");
        item.setWaveNumber(0);
        item.setEligibleAt(now.minusSeconds(10));
        item.setMatchingDeadlineAt(now.plusMinutes(5));
        item.setState(DispatchPoolItem.State.CLAIMED);
        item.setClaimedRoundId(roundId);
        item.setCreatedAt(now);
        item.setUpdatedAt(now);
        return item;
    }

    private record Stop(int sequence, Coordinate coordinate) { }
}
