package com.delivery.match_service.dto.event;

import com.delivery.match_service.common.constants.KafkaTopicConstants;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Read-only explanation of one Match decision. This event is deliberately
 * separate from shipper.found/not-found: observers can lose it without
 * changing the business outcome, while a replay can still correlate it by
 * command and matching session identity.
 */
@Data
@NoArgsConstructor
public class MatchingDecisionTraceEvent {

    public static final String TOPIC = KafkaTopicConstants.MATCHING_DECISION_TRACE_TOPIC;
    public static final String EVENT_TYPE = "MATCHING_DECISION_TRACE";
    public static final int EVENT_VERSION = 1;
    public static final String ALGORITHM_ID = "nearest-cod";
    public static final String ALGORITHM_VERSION = "v1";

    private UUID eventId;
    private UUID commandEventId;
    private String matchingSessionId;
    private Long orderId;
    private Long deliveryId;
    private String eventType = EVENT_TYPE;
    private int eventVersion = EVENT_VERSION;
    private String algorithmId = ALGORITHM_ID;
    private String algorithmVersion = ALGORITHM_VERSION;
    private String executionMode = "REAL";
    private String mode = "ACTIVE";
    private String decision;
    private Double pickupLat;
    private Double pickupLng;
    private Double radiusKm;
    private Integer candidatePoolSize;
    private Integer attempts;
    private Long latencyMs;
    private Instant occurredAt;
    private Long selectedShipperId;
    private boolean candidateViewIsPostGeoFilter = true;
    private List<String> notes = new ArrayList<>();
    private List<Stage> stages = new ArrayList<>();
    private List<Candidate> candidates = new ArrayList<>();

    @Data
    @NoArgsConstructor
    public static class Stage {
        private String name;
        private String result;
        private Integer candidateCount;
        private Long latencyMs;
        private String detail;
    }

    @Data
    @NoArgsConstructor
    public static class Candidate {
        private Long shipperId;
        private Double latitude;
        private Double longitude;
        private Double distanceKm;
        private Long completedDeliveries;
        private Double combinedScoreMinutes;
        private Boolean online;
        private Boolean codEligible;
        private Integer rank;
        private String state;
        private List<String> reasons = new ArrayList<>();
    }
}
