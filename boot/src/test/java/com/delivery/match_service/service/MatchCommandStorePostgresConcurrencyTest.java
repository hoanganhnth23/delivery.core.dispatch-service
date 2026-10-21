package com.delivery.match_service.service;

import com.delivery.match_service.MatchServiceApplication;
import com.delivery.match_service.dto.event.FindShipperEvent;
import com.delivery.match_service.entity.MatchCommand;
import com.delivery.match_service.repository.MatchCancellationTombstoneRepository;
import com.delivery.match_service.repository.MatchCommandRepository;
import com.delivery.match_service.repository.MatchOutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the retry convergence of the two independently ordered Kafka topics
 * against PostgreSQL's real serializable isolation, rather than H2 emulation.
 */
@SpringBootTest(classes = MatchServiceApplication.class, properties = {
        "spring.kafka.listener.auto-startup=false",
        "match.kafka.listener.auto-startup=false",
        "app.outbox.relay-enabled=false",
        "spring.cloud.discovery.enabled=false",
        "eureka.client.enabled=false"
})
@Testcontainers(disabledWithoutDocker = true)
class MatchCommandStorePostgresConcurrencyTest {

    private static final UUID FIND_EVENT_ID =
            UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID STOP_EVENT_ID =
            UUID.fromString("22222222-2222-2222-2222-222222222222");
    private static final UUID MATCHING_SESSION_ID =
            UUID.fromString("33333333-3333-3333-3333-333333333333");

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("match_concurrency")
            .withUsername("match")
            .withPassword("match");

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "validate");
        registry.add("spring.flyway.enabled", () -> "true");
        registry.add("spring.flyway.baseline-on-migrate", () -> "true");
    }

    @Autowired private MatchCommandStore store;
    @Autowired private MatchCommandRepository commandRepository;
    @Autowired private MatchCancellationTombstoneRepository tombstoneRepository;
    @Autowired private MatchOutboxEventRepository outboxRepository;
    @Autowired private ObjectMapper objectMapper;

    @BeforeEach
    void clean() {
        outboxRepository.deleteAll();
        commandRepository.deleteAll();
        tombstoneRepository.deleteAll();
    }

    @Test
    void concurrentStopAndFindConvergeToOneCancelledGenerationAfterReplay() throws Exception {
        FindShipperEvent find = findCommand();
        String findPayload = objectMapper.writeValueAsString(find);
        String stopPayload = stopPayload();

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Throwable> findResult = executor.submit(() -> invokeTogether(ready, start,
                    () -> store.acceptFindCommand("saga.command.find-shipper", findPayload, find)));
            Future<Throwable> stopResult = executor.submit(() -> invokeTogether(ready, start,
                    () -> store.recordStopMatching(STOP_EVENT_ID, 456L, 123L,
                            MATCHING_SESSION_ID, stopPayload)));

            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            // PostgreSQL may abort one SERIALIZABLE transaction. Kafka retries
            // the unchanged source record, which must converge without GEO work.
            List<Throwable> transientFailures = new ArrayList<>();
            Throwable findFailure = findResult.get(30, TimeUnit.SECONDS);
            Throwable stopFailure = stopResult.get(30, TimeUnit.SECONDS);
            if (findFailure != null) transientFailures.add(findFailure);
            if (stopFailure != null) transientFailures.add(stopFailure);
            assertThat(transientFailures).allSatisfy(failure ->
                    assertThat(failure).isInstanceOf(RuntimeException.class));

            store.recordStopMatching(STOP_EVENT_ID, 456L, 123L, MATCHING_SESSION_ID, stopPayload);
            assertThat(store.acceptFindCommand("saga.command.find-shipper", findPayload, find).mode())
                    .isEqualTo(MatchCommandStore.CommandMode.TERMINAL);
        } finally {
            executor.shutdownNow();
        }

        assertThat(commandRepository.findById(FIND_EVENT_ID)).get()
                .extracting(MatchCommand::getStatus)
                .isEqualTo(MatchCommand.Status.CANCELLED);
        assertThat(tombstoneRepository.findById(STOP_EVENT_ID)).isPresent();
        assertThat(outboxRepository.count()).isZero();
    }

    private FindShipperEvent findCommand() {
        FindShipperEvent event = new FindShipperEvent();
        event.setEventId(FIND_EVENT_ID);
        event.setOrderId(456L);
        event.setDeliveryId(123L);
        event.setMatchingSessionId(MATCHING_SESSION_ID);
        return event;
    }

    private String stopPayload() throws Exception {
        var payload = objectMapper.createObjectNode();
        payload.put("eventId", STOP_EVENT_ID.toString());
        payload.put("orderId", 456L);
        payload.put("deliveryId", 123L);
        payload.put("matchingSessionId", MATCHING_SESSION_ID.toString());
        return objectMapper.writeValueAsString(payload);
    }

    private Throwable invokeTogether(
            CountDownLatch ready,
            CountDownLatch start,
            ThrowingOperation operation) {
        try {
            ready.countDown();
            if (!start.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Concurrent PostgreSQL test did not start");
            }
            operation.run();
            return null;
        } catch (Throwable failure) {
            return failure;
        }
    }

    @FunctionalInterface
    private interface ThrowingOperation {
        void run() throws Exception;
    }
}
