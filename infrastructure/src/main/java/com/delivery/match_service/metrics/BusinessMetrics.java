package com.delivery.match_service.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class BusinessMetrics {
    private final MeterRegistry registry;
    public BusinessMetrics(MeterRegistry registry) { this.registry = registry; }
    public void record(String event) {
        Counter.builder("delivery.business.events").tag("event", event).tag("outcome", "success")
                .register(registry).increment();
    }
    public void kafka(String event) {
        Counter.builder("delivery.kafka.events").tag("event", event).register(registry).increment();
    }

    /** Aggregate rollout evidence without tagging request, order or shipper identifiers. */
    public void matchingAlgorithmDecision(String algorithm, String version, boolean simulation) {
        Counter.builder("delivery.matching.algorithm.decisions")
                .tag("algorithm", algorithm)
                .tag("version", version)
                .tag("execution_mode", simulation ? "SIMULATION" : "REAL")
                .register(registry)
                .increment();
    }
}
