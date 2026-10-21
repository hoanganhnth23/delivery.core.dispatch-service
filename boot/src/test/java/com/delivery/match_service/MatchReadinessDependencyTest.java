package com.delivery.match_service;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.reactive.server.WebTestClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "management.server.port=0",
                "management.endpoints.web.exposure.include=health",
                "management.endpoint.health.probes.enabled=true",
                "management.health.livenessstate.enabled=true",
                "management.health.readinessstate.enabled=true",
                "management.endpoint.health.group.liveness.include=livenessState",
                "management.endpoint.health.group.readiness.include=*",
                "management.endpoint.health.show-details=never",
                "management.endpoint.health.show-components=never",
                "spring.kafka.listener.auto-startup=false",
                "spring.data.redis.host=192.0.2.1",
                "spring.data.redis.port=6379",
                "spring.data.redis.timeout=100ms"
        })
class MatchReadinessDependencyTest {

    @LocalManagementPort
    private int managementPort;

    @Test
    void becomesNotReadyWhenRedisIsUnavailable() {
        WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + managementPort)
                .build()
                .get().uri("/actuator/health/readiness")
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
                .expectBody()
                .jsonPath("$.status").isEqualTo("DOWN")
                .jsonPath("$.components").doesNotExist();
    }
}
