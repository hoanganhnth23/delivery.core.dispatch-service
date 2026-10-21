package com.delivery.match_service.algorithm;

import com.delivery.match_service.config.MatchingAlgorithmProperties;
import com.delivery.match_service.dto.response.NearbyShipperResponse;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class BalancedEtaCanaryPolicyTest {

    @Test
    void staysOnNearestProfileWhenDisabledOrAtZeroPercent() {
        MatchingAlgorithmProperties properties = new MatchingAlgorithmProperties();
        BalancedEtaCanaryPolicy policy = new BalancedEtaCanaryPolicy(properties);

        assertThat(policy.select(UUID.randomUUID()).id()).isEqualTo("nearest-cod");

        properties.setEnabled(true);
        properties.setCanaryPercent(0);
        assertThat(policy.select(UUID.randomUUID()).id()).isEqualTo("nearest-cod");
    }

    @Test
    void usesStableEventIdForFullCanarySelection() {
        MatchingAlgorithmProperties properties = new MatchingAlgorithmProperties();
        properties.setEnabled(true);
        properties.setCanaryPercent(100);
        BalancedEtaCanaryPolicy policy = new BalancedEtaCanaryPolicy(properties);
        UUID eventId = UUID.fromString("11111111-1111-1111-1111-111111111111");

        assertThat(policy.select(eventId)).isEqualTo(policy.select(eventId));
        assertThat(policy.select(eventId).id()).isEqualTo("balanced-eta");
        assertThat(policy.select(eventId).version()).isEqualTo("v1");
    }

    @Test
    void ranksByEtaPlusCompletedDeliveryFairnessPenalty() {
        MatchingAlgorithmProperties properties = new MatchingAlgorithmProperties();
        properties.setEnabled(true);
        properties.setCanaryPercent(100);
        properties.setEtaSpeedKmPerMinute(0.5d);
        properties.setFairnessPenaltyMinutesPerCompletedDelivery(0.03d);
        BalancedEtaCanaryPolicy policy = new BalancedEtaCanaryPolicy(properties);

        NearbyShipperResponse nearButBusy = shipper(14L, 0.1716d, 20L);
        NearbyShipperResponse fartherButFair = shipper(15L, 0.3272d, 0L);

        List<NearbyShipperResponse> ranked = policy.rank(
                policy.select(UUID.randomUUID()), List.of(nearButBusy, fartherButFair));

        assertThat(ranked).extracting(NearbyShipperResponse::getShipperId)
                .containsExactly(15L, 14L);
        assertThat(nearButBusy.getCombinedScoreMinutes()).isEqualTo(0.9432d);
        assertThat(fartherButFair.getCombinedScoreMinutes()).isEqualTo(0.6544d);
    }

    private NearbyShipperResponse shipper(Long id, double distanceKm, long completedDeliveries) {
        NearbyShipperResponse response = new NearbyShipperResponse(
                id, null, null, 10.76d, 106.66d, distanceKm, true, null);
        response.setCompletedDeliveries(completedDeliveries);
        return response;
    }
}
