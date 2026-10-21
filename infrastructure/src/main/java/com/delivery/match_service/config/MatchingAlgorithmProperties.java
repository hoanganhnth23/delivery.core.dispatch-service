package com.delivery.match_service.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Guarded rollout controls for an active matching profile. Defaults are
 * deliberately fail-closed: production keeps nearest-cod/v1 until an operator
 * explicitly enables a non-zero canary.
 */
@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "matching.algorithm.balanced-eta")
public class MatchingAlgorithmProperties {
    private boolean enabled;
    private int canaryPercent;
    private double fairnessPenaltyMinutesPerCompletedDelivery = 0.03d;
    private double etaSpeedKmPerMinute = 0.5d;
}
