package com.delivery.match_service.config;

import com.delivery.routing.client.HttpRoutingClient;
import java.net.URI;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

/** Wires the typed routing boundary while leaving route policy to Match. */
@Configuration
public class RoutingClientConfig {

    private static final int ROUTING_TIMEOUT_MS = 500;

    @Bean("platformRoutingClient")
    @Lazy
    com.delivery.routing.client.RoutingClient platformRoutingClient(
            @Value("${routing.service.url:http://routing-service:8094}") String routingServiceUrl,
            @Value("${app.internal.secret:}") String internalSecret) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(ROUTING_TIMEOUT_MS);
        requestFactory.setReadTimeout(ROUTING_TIMEOUT_MS);
        RestClient restClient = RestClient.builder()
                .requestFactory(requestFactory)
                .build();
        return new HttpRoutingClient(restClient, URI.create(routingServiceUrl), internalSecret);
    }
}
