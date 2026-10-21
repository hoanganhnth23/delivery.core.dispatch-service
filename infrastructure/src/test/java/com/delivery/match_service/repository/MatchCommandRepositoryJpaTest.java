package com.delivery.match_service.repository;

import com.delivery.match_service.entity.MatchCommand;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.test.context.ContextConfiguration;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest(properties = {
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.flyway.enabled=false"
})
@ContextConfiguration(classes = MatchCommandRepositoryJpaTest.JpaTestApplication.class)
class MatchCommandRepositoryJpaTest {

    @org.springframework.beans.factory.annotation.Autowired
    private MatchCommandRepository repository;

    @Test
    void persistsAndLocksCommandThroughInfrastructureRepository() {
        UUID eventId = UUID.randomUUID();
        UUID sessionId = UUID.randomUUID();
        repository.saveAndFlush(new MatchCommand(
                eventId,
                "saga.command.find-shipper",
                41L,
                42L,
                sessionId,
                "{}",
                "fingerprint"));

        MatchCommand stored = repository.findByEventIdForUpdate(eventId).orElseThrow();

        assertThat(stored.getDeliveryId()).isEqualTo(42L);
        assertThat(stored.getMatchingSessionId()).isEqualTo(sessionId);
        assertThat(stored.getStatus()).isEqualTo(MatchCommand.Status.PENDING);
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EntityScan("com.delivery.match_service.entity")
    @EnableJpaRepositories("com.delivery.match_service.repository")
    static class JpaTestApplication {
    }
}
