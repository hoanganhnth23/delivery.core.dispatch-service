package com.delivery.match_service.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.delivery.routing.client.HttpRoutingClient;
import org.junit.jupiter.api.Test;

class RoutingClientConfigTest {

    @Test
    void createsPlatformRoutingClientFromConfiguredUrlAndSecret() {
        com.delivery.routing.client.RoutingClient client = new RoutingClientConfig()
                .platformRoutingClient("http://routing-service:8094", "test-internal-secret");

        assertThat(client).isInstanceOf(HttpRoutingClient.class);
    }
}
