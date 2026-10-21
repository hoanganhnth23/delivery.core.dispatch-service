package com.delivery.match_service.service.impl;

import com.delivery.match_service.service.SettlementEligibilityClient;
import com.delivery.match_service.config.MatchSettlementCircuitBreaker;
import com.delivery.match_service.config.SettlementCallResilienceProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatusCode;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class SettlementEligibilityClientImpl implements SettlementEligibilityClient {

    private final WebClient webClient;
    private final MatchSettlementCircuitBreaker circuitBreaker;
    private final SettlementCallResilienceProperties resilienceProperties;

    public SettlementEligibilityClientImpl(
            @Qualifier("settlementServiceWebClient") WebClient webClient,
            MatchSettlementCircuitBreaker circuitBreaker,
            SettlementCallResilienceProperties resilienceProperties) {
        this.webClient = webClient;
        this.circuitBreaker = circuitBreaker;
        this.resilienceProperties = resilienceProperties;
    }

    @Override
    public Mono<Boolean> isCodEligible(Long shipperId, BigDecimal codAmount) {
        return webClient.get()
                .uri(uri -> uri.path("/api/settlement/internal/shippers/{shipperId}/cod-eligibility")
                        .queryParam("codAmount", codAmount.toPlainString())
                        .build(shipperId))
                .retrieve()
                .onStatus(HttpStatusCode::isError, response -> response.createException().flatMap(Mono::error))
                .bodyToMono(new ParameterizedTypeReference<InternalBaseResponse<Boolean>>() {
                })
                .switchIfEmpty(Mono.error(new IllegalStateException("Settlement eligibility response is empty")))
                .flatMap(response -> {
                    if (response.status() != 1 || response.data() == null) {
                        return Mono.error(new IllegalStateException("Invalid settlement eligibility response"));
                    }
                    return Mono.just(response.data());
                })
                .timeout(java.time.Duration.ofMillis(resilienceProperties.getTimeoutMs()))
                .transformDeferred(CircuitBreakerOperator.of(circuitBreaker.circuitBreaker()));
    }

    @Override
    public Mono<List<SettlementEligibilityClient.CodCapacityHoldRef>> createCodCapacityHolds(
            Long shipperId, UUID matchingSessionId, UUID waveId, UUID eventId,
            List<SettlementEligibilityClient.CodCapacityHoldRequestItem> offers) {
        CapacityHoldRequest request = new CapacityHoldRequest(eventId, shipperId, matchingSessionId,
                waveId, offers);
        return webClient.post()
                .uri("/api/settlement/internal/cod-capacity/holds")
                .bodyValue(request)
                .retrieve()
                .onStatus(HttpStatusCode::isError, response -> response.createException().flatMap(Mono::error))
                .bodyToMono(new ParameterizedTypeReference<InternalBaseResponse<List<CapacityHoldResponse>>>() { })
                .switchIfEmpty(Mono.error(new IllegalStateException("Settlement hold response is empty")))
                .flatMap(response -> {
                    if (response.status() != 1 || response.data() == null || response.data().isEmpty()) {
                        return Mono.error(new IllegalStateException("Invalid settlement hold response"));
                    }
                    return Mono.just(response.data().stream()
                            .map(item -> new SettlementEligibilityClient.CodCapacityHoldRef(
                                    item.holdId(), item.offerId(), item.orderId(), item.deliveryId()))
                            .toList());
                })
                .timeout(java.time.Duration.ofMillis(resilienceProperties.getTimeoutMs()))
                .transformDeferred(CircuitBreakerOperator.of(circuitBreaker.circuitBreaker()));
    }

    @Override
    public Mono<Boolean> transitionCodCapacityHold(UUID holdId, String target) {
        String endpoint = switch (target.toUpperCase(java.util.Locale.ROOT)) {
            case "COMMITTED" -> "commit";
            case "RELEASED" -> "release";
            default -> throw new IllegalArgumentException("Unsupported COD hold target: " + target);
        };
        return webClient.post()
                .uri(uri -> uri.path("/api/settlement/internal/cod-capacity/holds/{holdId}/{target}")
                        .build(holdId, endpoint))
                .retrieve()
                .onStatus(HttpStatusCode::isError, response -> response.createException().flatMap(Mono::error))
                .bodyToMono(new ParameterizedTypeReference<InternalBaseResponse<Object>>() { })
                .switchIfEmpty(Mono.error(new IllegalStateException("Settlement hold transition response is empty")))
                .map(response -> response.status() == 1);
    }

    private record CapacityHoldRequest(UUID eventId, Long shipperId, UUID matchingSessionId,
                                       UUID waveId,
                                       List<SettlementEligibilityClient.CodCapacityHoldRequestItem> offers) {
    }

    private record CapacityHoldResponse(UUID holdId, UUID offerId, Long orderId, Long deliveryId) {
    }
}
