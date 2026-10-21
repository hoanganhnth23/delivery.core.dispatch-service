package com.delivery.match_service.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Operator-controlled guardrails for rolling dispatch. The feature is off by
 * default until all participating services understand the batch contract.
 */
@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "matching.batch")
public class MatchingBatchProperties {

    private boolean enabled;
    private boolean schedulerEnabled;
    private int windowSeconds = 5;
    private int maxOrders = 3;
    private int maxOrdersPerRound = 50;
    private int maxShippersPerRound = 100;
    private int maxEtaDetourSeconds = 600;
    private int neighborRing = 1;
    private int maxShippersPerWave = 3;
    private int maxBundleCandidatesPerShipper = 100;
    private int bundleSeedOrdersPerShipper = 12;
    private int maxWaves = 3;
    private int waveTimeoutSeconds = 20;
    private int expiryBatchSize = 100;
    private int canaryPercent;
    private boolean clientCapabilityRequired = true;
}
