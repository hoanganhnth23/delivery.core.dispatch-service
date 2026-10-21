package com.delivery.match_service.algorithm;

import com.delivery.match.domain.single.CandidatePolicy;
import com.delivery.match_service.config.MatchingAlgorithmProperties;
import com.delivery.match_service.dto.response.NearbyShipperResponse;
import org.springframework.stereotype.Component;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Maps host configuration and DTOs into the framework-free candidate policy. */
@Component
public class BalancedEtaCanaryPolicy {
    public static final Profile NEAREST_COD = new Profile("nearest-cod", "v1");
    public static final Profile BALANCED_ETA = new Profile("balanced-eta", "v1");

    private final MatchingAlgorithmProperties properties;

    public BalancedEtaCanaryPolicy(MatchingAlgorithmProperties properties) {
        this.properties = properties;
    }

    public Profile select(UUID eventId) {
        CandidatePolicy.Profile selected = CandidatePolicy.select(
                properties.isEnabled(), properties.getCanaryPercent(), eventId);
        return new Profile(selected.id(), selected.version());
    }

    public List<NearbyShipperResponse> rank(Profile profile, List<NearbyShipperResponse> candidates) {
        Map<CandidatePolicy.Candidate, NearbyShipperResponse> sources = new IdentityHashMap<>();
        List<CandidatePolicy.Candidate> snapshots = candidates == null ? null : candidates.stream()
                .map(source -> {
                    CandidatePolicy.Candidate snapshot = new CandidatePolicy.Candidate(
                            source.getShipperId(), source.getDistanceKm(), source.getCompletedDeliveries());
                    sources.put(snapshot, source);
                    return snapshot;
                }).toList();
        CandidatePolicy.Profile coreProfile = profile == null ? null
                : new CandidatePolicy.Profile(profile.id(), profile.version());
        return CandidatePolicy.rank(coreProfile, snapshots, properties.getEtaSpeedKmPerMinute(),
                        properties.getFairnessPenaltyMinutesPerCompletedDelivery()).stream()
                .map(ranked -> {
                    NearbyShipperResponse source = sources.get(ranked.candidate());
                    if (ranked.score() != null) source.setCombinedScoreMinutes(ranked.score());
                    return source;
                }).collect(java.util.stream.Collectors.toList());
    }

    public record Profile(String id, String version) { }
}
