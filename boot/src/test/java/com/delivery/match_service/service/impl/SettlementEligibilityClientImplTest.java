package com.delivery.match_service.service.impl;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import com.delivery.match_service.config.MatchSettlementCircuitBreaker;
import com.delivery.match_service.config.SettlementCallResilienceProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SettlementEligibilityClientImplTest {

    @Test
    void mapsCapacityHoldIdentityAndRejectsInvalidEnvelopes() {
        java.util.UUID hold = java.util.UUID.randomUUID();
        java.util.UUID offer = java.util.UUID.randomUUID();
        assertThat(clientWithJson("{\"status\":1,\"data\":[{\"holdId\":\"" + hold
                + "\",\"offerId\":\"" + offer + "\",\"orderId\":12,\"deliveryId\":13}]}")
                .createCodCapacityHolds(10L, hold, offer, hold, java.util.List.of()).block())
                .containsExactly(new com.delivery.match_service.service.SettlementEligibilityClient.CodCapacityHoldRef(
                        hold, offer, 12L, 13L));
        for (String json : java.util.List.of("{\"status\":0,\"data\":[]}",
                "{\"status\":1,\"data\":null}", "{\"status\":1,\"data\":[]}")) {
            assertThatThrownBy(() -> clientWithJson(json)
                    .createCodCapacityHolds(10L, hold, offer, hold, java.util.List.of()).block())
                    .isInstanceOf(IllegalStateException.class).hasMessage("Invalid settlement hold response");
        }
        assertThatThrownBy(() -> clientWithResponse(ClientResponse.create(HttpStatus.OK).build())
                .createCodCapacityHolds(10L, hold, offer, hold, java.util.List.of()).block())
                .isInstanceOf(IllegalStateException.class).hasMessage("Settlement hold response is empty");
    }

    @Test
    void transitionsCommitAndReleaseAndRejectsUnsupportedTargets() {
        java.util.UUID hold = java.util.UUID.randomUUID();
        for (String target : java.util.List.of("committed", "RELEASED")) {
            assertThat(clientWithJson("{\"status\":1}").transitionCodCapacityHold(hold, target).block()).isTrue();
            assertThat(clientWithJson("{\"status\":0}").transitionCodCapacityHold(hold, target).block()).isFalse();
        }
        assertThatThrownBy(() -> clientWithJson("{}").transitionCodCapacityHold(hold, "PENDING"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("Unsupported COD hold target: PENDING");
        assertThatThrownBy(() -> clientWithResponse(ClientResponse.create(HttpStatus.OK).build())
                .transitionCodCapacityHold(hold, "COMMITTED").block())
                .isInstanceOf(IllegalStateException.class).hasMessage("Settlement hold transition response is empty");
    }

    @Test
    void propagatesHttpFailuresForEveryOperation() {
        java.util.UUID identity = java.util.UUID.randomUUID();
        SettlementEligibilityClientImpl client = clientWithResponse(ClientResponse.create(HttpStatus.SERVICE_UNAVAILABLE).build());
        assertThatThrownBy(() -> client.isCodEligible(10L, BigDecimal.ONE).block())
                .isInstanceOf(org.springframework.web.reactive.function.client.WebClientResponseException.ServiceUnavailable.class);
        assertThatThrownBy(() -> client.createCodCapacityHolds(10L, identity, identity, identity, java.util.List.of()).block())
                .isInstanceOf(org.springframework.web.reactive.function.client.WebClientResponseException.ServiceUnavailable.class);
        assertThatThrownBy(() -> client.transitionCodCapacityHold(identity, "RELEASED").block())
                .isInstanceOf(org.springframework.web.reactive.function.client.WebClientResponseException.ServiceUnavailable.class);
    }

    @Test
    void returnsCanonicalBooleanData() {
        assertThat(clientWithJson("{\"status\":1,\"message\":\"Eligible\",\"data\":true}")
                .isCodEligible(10L, new BigDecimal("100000"))
                .block()).isTrue();
        assertThat(clientWithJson("{\"status\":1,\"message\":\"Denied\",\"data\":false}")
                .isCodEligible(10L, new BigDecimal("100000"))
                .block()).isFalse();
    }

    @Test
    void rejectsFailureAndNullDataEnvelopes() {
        assertInvalid("{\"status\":0,\"message\":\"Failure\",\"data\":true}");
        assertInvalid("{\"status\":1,\"message\":\"Malformed\",\"data\":null}");
    }

    @Test
    void rejectsEmptyBody() {
        SettlementEligibilityClientImpl client = clientWithResponse(
                ClientResponse.create(HttpStatus.OK).build());

        assertThatThrownBy(() -> client.isCodEligible(10L, BigDecimal.ONE).block())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("empty");
    }

    private void assertInvalid(String json) {
        assertThatThrownBy(() -> clientWithJson(json)
                .isCodEligible(10L, BigDecimal.ONE)
                .block())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Invalid settlement eligibility response");
    }

    private SettlementEligibilityClientImpl clientWithJson(String json) {
        return clientWithResponse(ClientResponse.create(HttpStatus.OK)
                .header("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                .body(json)
                .build());
    }

    private SettlementEligibilityClientImpl clientWithResponse(ClientResponse response) {
        WebClient webClient = WebClient.builder()
                .baseUrl("http://settlement-service")
                .exchangeFunction(request -> Mono.just(response))
                .build();
        SettlementCallResilienceProperties properties = new SettlementCallResilienceProperties();
        return new SettlementEligibilityClientImpl(webClient,
                new MatchSettlementCircuitBreaker(properties, new SimpleMeterRegistry()), properties);
    }
}
