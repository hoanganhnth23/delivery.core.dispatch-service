package com.delivery.match_service.config;

import java.time.Duration;

import org.springframework.stereotype.Component;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.micrometer.core.instrument.MeterRegistry;

/** Reactive circuit boundary for Match -> Settlement COD eligibility checks. */
@Component
public class MatchSettlementCircuitBreaker {
    private final CircuitBreaker circuitBreaker;

    public MatchSettlementCircuitBreaker(SettlementCallResilienceProperties properties, MeterRegistry meterRegistry) {
        circuitBreaker = CircuitBreaker.of("matchSettlementService", CircuitBreakerConfig.custom()
                .failureRateThreshold(properties.getFailureRateThreshold())
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(properties.getSlidingWindowSize())
                .minimumNumberOfCalls(properties.getSlidingWindowSize())
                .waitDurationInOpenState(Duration.ofSeconds(properties.getWaitOpenSeconds()))
                .permittedNumberOfCallsInHalfOpenState(properties.getPermittedHalfOpenCalls())
                .build());
        circuitBreaker.getEventPublisher().onStateTransition(event -> meterRegistry.counter(
                "delivery.circuit.state_transition", "circuit", "matchSettlementService",
                "transition", event.getStateTransition().name()).increment());
    }

    public CircuitBreaker circuitBreaker() { return circuitBreaker; }
}
