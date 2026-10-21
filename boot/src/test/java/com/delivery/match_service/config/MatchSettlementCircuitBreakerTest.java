package com.delivery.match_service.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class MatchSettlementCircuitBreakerTest {
    @Test
    void recordsFailuresAndUsesConfiguredHalfOpenCapacity() {
        SettlementCallResilienceProperties properties = new SettlementCallResilienceProperties();
        properties.setSlidingWindowSize(2);
        properties.setPermittedHalfOpenCalls(1);
        MatchSettlementCircuitBreaker breaker = new MatchSettlementCircuitBreaker(properties, new SimpleMeterRegistry());

        breaker.circuitBreaker().onError(1, java.util.concurrent.TimeUnit.MILLISECONDS,
                new IllegalStateException("settlement 5xx"));
        breaker.circuitBreaker().onError(1, java.util.concurrent.TimeUnit.MILLISECONDS,
                new java.util.concurrent.TimeoutException("settlement timeout"));
        assertThat(breaker.circuitBreaker().getState()).isEqualTo(CircuitBreaker.State.OPEN);

        breaker.circuitBreaker().transitionToHalfOpenState();
        breaker.circuitBreaker().onSuccess(1, java.util.concurrent.TimeUnit.MILLISECONDS);
        assertThat(breaker.circuitBreaker().getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }
}
