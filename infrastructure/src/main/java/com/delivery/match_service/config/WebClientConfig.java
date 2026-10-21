package com.delivery.match_service.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.beans.factory.annotation.Qualifier;
import reactor.netty.http.client.HttpClient;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;

/**
 * ✅ WebClient Configuration cho HTTP calls đến các services khác
 * Theo Backend Instructions: Proper configuration classes
 */
@Configuration
public class WebClientConfig {

    private final SettlementCallResilienceProperties resilienceProperties;

    public WebClientConfig(SettlementCallResilienceProperties resilienceProperties) {
        this.resilienceProperties = resilienceProperties;
    }

    @Value("${settlement.service.url}")
    private String settlementServiceUrl;

    @Value("${app.internal.secret:}")
    private String internalSecret;

    @Bean
    @LoadBalanced
    @Qualifier("settlementServiceWebClientBuilder")
    public WebClient.Builder settlementServiceWebClientBuilder() {
        return WebClient.builder();
    }

    @Bean
    @Qualifier("settlementServiceWebClient")
    public WebClient settlementServiceWebClient(
            @Qualifier("settlementServiceWebClientBuilder") WebClient.Builder webClientBuilder) {
        return webClientBuilder
                .baseUrl(settlementServiceUrl)
                .defaultHeader("Internal-Token", internalSecret)
                .clientConnector(new ReactorClientHttpConnector(HttpClient.create()
                        .responseTimeout(java.time.Duration.ofMillis(resilienceProperties.getTimeoutMs()))))
                .build();
    }

}
