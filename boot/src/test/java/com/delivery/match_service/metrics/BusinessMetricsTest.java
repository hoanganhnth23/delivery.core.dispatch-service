package com.delivery.match_service.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BusinessMetricsTest {

    @Test
    void recordsAlgorithmDecisionsWithoutHighCardinalityIdentifiers() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        BusinessMetrics metrics = new BusinessMetrics(registry);

        metrics.matchingAlgorithmDecision("balanced-eta", "v1", true);

        assertThat(registry.get("delivery.matching.algorithm.decisions")
                .tag("algorithm", "balanced-eta")
                .tag("version", "v1")
                .tag("execution_mode", "SIMULATION")
                .counter().count()).isEqualTo(1d);
    }
}
