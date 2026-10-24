package com.delivery.saga_orchestrator_service.service;

import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class SagaOutboxMigrationTest {

    @Test
    void migrationCreatesUsableSchemaWithoutHibernate() {
        var dataSource = new DriverManagerDataSource(
                "jdbc:h2:mem:saga_outbox_migration;MODE=PostgreSQL;DB_CLOSE_DELAY=-1", "sa", "");
        new ResourceDatabasePopulator(
                new ClassPathResource("db/migration/V1__create_saga_outbox.sql"))
                .execute(dataSource);
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        LocalDateTime now = LocalDateTime.now();

        jdbc.update("""
                INSERT INTO saga_outbox_events(event_id, aggregate_id, event_type,
                    topic, event_key, payload, status, attempts, next_attempt_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, UUID.randomUUID(), "42", SagaManager.CMD_CREATE_DELIVERY,
                SagaManager.CMD_CREATE_DELIVERY, "42", "{}", "PENDING", 0, now, now);

        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM saga_outbox_events", Long.class)).isEqualTo(1L);
    }
}
