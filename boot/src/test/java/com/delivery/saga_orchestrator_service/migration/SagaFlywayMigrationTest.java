package com.delivery.saga_orchestrator_service.migration;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SagaFlywayMigrationTest {

    @Test
    void cleanSchemaCreatesAggregateHistoryAndUniqueOrderBoundary() throws Exception {
        String url = databaseUrl("clean");
        migrate(url);

        try (Connection connection = DriverManager.getConnection(url, "sa", "");
             Statement statement = connection.createStatement()) {
            UUID firstSaga = UUID.randomUUID();
            insertSaga(statement, firstSaga, 42);
            insertStep(statement, firstSaga);
            assertThatThrownBy(() -> insertSaga(statement, UUID.randomUUID(), 42))
                    .isInstanceOf(Exception.class)
                    .hasMessageContaining("Unique");
            assertThat(count(statement, "SELECT count(*) FROM saga_steps")).isEqualTo(1);
        }
    }

    @Test
    void legacySchemaAddsVersionAndPreservesAggregateAndStep() throws Exception {
        String url = databaseUrl("legacy");
        createLegacySchema(url, false, false);
        UUID sagaId = UUID.randomUUID();
        try (Connection connection = DriverManager.getConnection(url, "sa", "");
             Statement statement = connection.createStatement()) {
            insertSagaWithoutVersion(statement, sagaId, 42);
            insertStep(statement, sagaId);
        }

        migrate(url);

        try (Connection connection = DriverManager.getConnection(url, "sa", "");
             Statement statement = connection.createStatement()) {
            assertThat(count(statement, "SELECT count(*) FROM saga_instances")).isEqualTo(1);
            assertThat(count(statement, "SELECT count(*) FROM saga_steps")).isEqualTo(1);
            try (ResultSet resultSet = statement.executeQuery(
                    "SELECT version FROM saga_instances WHERE order_id = 42")) {
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getLong(1)).isZero();
            }
        }
    }

    @Test
    void duplicateLegacyOrdersStopMigration() throws Exception {
        String url = databaseUrl("duplicate");
        createLegacySchema(url, false, false);
        try (Connection connection = DriverManager.getConnection(url, "sa", "");
             Statement statement = connection.createStatement()) {
            insertSagaWithoutVersion(statement, UUID.randomUUID(), 42);
            insertSagaWithoutVersion(statement, UUID.randomUUID(), 42);
        }

        assertThatThrownBy(() -> migrate(url))
                .isInstanceOf(FlywayException.class)
                .rootCause()
                .hasMessageContaining("duplicate order_id");
    }

    @Test
    void missingCoreColumnStopsMigrationInsteadOfGuessingState() throws Exception {
        String url = databaseUrl("missing_core");
        createLegacySchema(url, true, false);

        assertThatThrownBy(() -> migrate(url))
                .isInstanceOf(FlywayException.class)
                .rootCause()
                .hasMessageContaining("missing core column")
                .hasMessageContaining("status");
    }

    @Test
    void incompleteLegacyTablePairStopsMigration() throws Exception {
        String url = databaseUrl("incomplete");
        createLegacySchema(url, false, true);

        assertThatThrownBy(() -> migrate(url))
                .isInstanceOf(FlywayException.class)
                .rootCause()
                .hasMessageContaining("schema is incomplete");
    }

    private void migrate(String url) {
        Flyway.configure()
                .dataSource(url, "sa", "")
                .baselineOnMigrate(true)
                .baselineVersion(MigrationVersion.fromVersion("0"))
                .load()
                .migrate();
    }

    private void createLegacySchema(String url, boolean omitStatus, boolean omitSteps) throws Exception {
        try (Connection connection = DriverManager.getConnection(url, "sa", "");
             Statement statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE saga_instances (
                        id UUID PRIMARY KEY,
                        saga_type VARCHAR(255) NOT NULL,
                        order_id BIGINT NOT NULL,
                        delivery_id BIGINT,
                        shipper_id BIGINT,
                        %s
                        payload JSONB,
                        created_at TIMESTAMP NOT NULL,
                        updated_at TIMESTAMP,
                        completed_at TIMESTAMP
                    )
                    """.formatted(omitStatus ? "" : "status VARCHAR(255) NOT NULL,"));
            if (!omitSteps) {
                statement.execute("""
                        CREATE TABLE saga_steps (
                            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                            saga_instance_id UUID NOT NULL,
                            step_name VARCHAR(255) NOT NULL,
                            event_type VARCHAR(255) NOT NULL,
                            event_data TEXT,
                            executed_at TIMESTAMP NOT NULL
                        )
                        """);
            }
        }
    }

    private void insertSaga(Statement statement, UUID id, long orderId) throws Exception {
        statement.executeUpdate("""
                INSERT INTO saga_instances
                    (id, version, saga_type, order_id, status, payload, created_at, updated_at)
                VALUES ('%s', 0, 'ORDER_CREATION', %d, 'STARTED', '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """.formatted(id, orderId));
    }

    private void insertSagaWithoutVersion(Statement statement, UUID id, long orderId) throws Exception {
        statement.executeUpdate("""
                INSERT INTO saga_instances
                    (id, saga_type, order_id, status, payload, created_at, updated_at)
                VALUES ('%s', 'ORDER_CREATION', %d, 'STARTED', '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """.formatted(id, orderId));
    }

    private void insertStep(Statement statement, UUID sagaId) throws Exception {
        statement.executeUpdate("""
                INSERT INTO saga_steps (saga_instance_id, step_name, event_type, event_data, executed_at)
                VALUES ('%s', 'ORDER_CREATED', 'order.created', '{}', CURRENT_TIMESTAMP)
                """.formatted(sagaId));
    }

    private long count(Statement statement, String sql) throws Exception {
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    private String databaseUrl(String label) {
        return "jdbc:h2:mem:saga_migration_" + label + "_" + UUID.randomUUID()
                + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE";
    }
}
