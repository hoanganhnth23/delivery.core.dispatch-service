package com.delivery.saga_orchestrator_service.service;

import com.delivery.saga_orchestrator_service.SagaOrchestratorServiceApplication;
import com.delivery.saga_orchestrator_service.entity.SagaInstance;
import com.delivery.saga_orchestrator_service.entity.SagaOutboxEvent;
import com.delivery.saga_orchestrator_service.repository.SagaInboundReceiptRepository;
import com.delivery.saga_orchestrator_service.repository.SagaInstanceRepository;
import com.delivery.saga_orchestrator_service.repository.SagaOutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exercises the Saga row lock and durable timeout inbox under real PostgreSQL
 * concurrency. A duplicate scheduler observation must enqueue compensation once.
 */
@SpringBootTest(classes = SagaOrchestratorServiceApplication.class, properties = {
        "spring.kafka.listener.auto-startup=false",
        "app.outbox.relay-enabled=false",
        "app.saga.timeout-poll-delay-ms=3600000",
        "app.saga.early-event-poll-delay-ms=3600000",
        "spring.cloud.discovery.enabled=false",
        "eureka.client.enabled=false"
})
@Testcontainers(disabledWithoutDocker = true)
class SagaTimeoutPostgresConcurrencyTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("saga_concurrency")
            .withUsername("saga")
            .withPassword("saga");

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

    @Autowired private SagaStuckStateTimeoutService stuckTimeouts;
    @Autowired private io.micrometer.core.instrument.MeterRegistry meters;
    @Autowired private SagaManager manager;
    @Autowired private SagaInstanceRepository sagaRepository;
    @Autowired private SagaInboundReceiptRepository receiptRepository;
    @Autowired private SagaOutboxEventRepository outboxRepository;
    @Autowired private PlatformTransactionManager transactionManager;

    @BeforeEach
    void clean() {
        outboxRepository.deleteAll();
        receiptRepository.deleteAll();
        sagaRepository.deleteAll();
    }

    @Test
    void duplicateTimeoutObservationProducesOneReceiptAndOneCompensationSet() throws Exception {
        SagaInstance saga = new SagaInstance();
        saga.setSagaType("ORDER_CREATION");
        saga.setOrderId(940001L);
        saga.setDeliveryId(940002L);
        saga.setStatus(SagaInstance.SagaStatus.FINDING_SHIPPER);
        saga.setPayload("{\"orderId\":940001,\"totalPrice\":120000,\"paymentMethod\":\"COD\"}");
        saga = sagaRepository.saveAndFlush(saga);
        SagaTimeoutCommand timeout = SagaTimeoutCommand.forStage(
                saga, SagaInstance.SagaStatus.FINDING_SHIPPER, Duration.ZERO, "matching timeout");

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Throwable> first = executor.submit(() -> invokeTogether(ready, start,
                    () -> manager.handleTimeout(timeout)));
            Future<Throwable> second = executor.submit(() -> invokeTogether(ready, start,
                    () -> manager.handleTimeout(timeout)));

            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            assertThat(first.get(30, TimeUnit.SECONDS)).isNull();
            assertThat(second.get(30, TimeUnit.SECONDS)).isNull();
        } finally {
            executor.shutdownNow();
        }

        assertThat(sagaRepository.findById(saga.getId())).get()
                .extracting(SagaInstance::getStatus)
                .isEqualTo(SagaInstance.SagaStatus.FAILED);
        assertThat(receiptRepository.count()).isEqualTo(1);
        assertThat(outboxRepository.findAll())
                .extracting(SagaOutboxEvent::getEventType)
                .containsExactlyInAnyOrder(
                        SagaManager.CMD_MARK_SHIPPER_NOT_FOUND,
                SagaManager.CMD_UPDATE_ORDER_STATUS);
    }

    @Test
    void twoSchedulerReplicasProduceOneTimeoutReceiptAndOneCompensationSet() throws Exception {
        SagaInstance saga = new SagaInstance();
        saga.setSagaType("ORDER_CREATION");
        saga.setOrderId(940011L);
        saga.setDeliveryId(940012L);
        saga.setStatus(SagaInstance.SagaStatus.FINDING_SHIPPER);
        saga.setPayload("{\"orderId\":940011,\"totalPrice\":120000,\"paymentMethod\":\"COD\"}");
        LocalDateTime overdue = LocalDateTime.now().minusMinutes(6);
        saga.setCreatedAt(overdue);
        saga.setUpdatedAt(overdue);
        saga = sagaRepository.saveAndFlush(saga);

        SagaTimeoutScheduler firstScheduler = new SagaTimeoutScheduler(sagaRepository, manager, 25);
        SagaTimeoutScheduler secondScheduler = new SagaTimeoutScheduler(sagaRepository, manager, 25);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Throwable> first = executor.submit(() -> invokeTogether(ready, start,
                    firstScheduler::checkTimeouts));
            Future<Throwable> second = executor.submit(() -> invokeTogether(ready, start,
                    secondScheduler::checkTimeouts));

            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            assertThat(first.get(30, TimeUnit.SECONDS)).isNull();
            assertThat(second.get(30, TimeUnit.SECONDS)).isNull();
        } finally {
            executor.shutdownNow();
        }

        assertThat(sagaRepository.findById(saga.getId())).get()
                .extracting(SagaInstance::getStatus)
                .isEqualTo(SagaInstance.SagaStatus.FAILED);
        assertThat(receiptRepository.count()).isEqualTo(1);
        assertThat(outboxRepository.findAll())
                .extracting(SagaOutboxEvent::getEventType)
                .containsExactlyInAnyOrder(
                        SagaManager.CMD_MARK_SHIPPER_NOT_FOUND,
                SagaManager.CMD_UPDATE_ORDER_STATUS);
    }

    @Test
    void twoOutboxRelayReplicasClaimAndSendOneCommandOnce() throws Exception {
        SagaOutboxEvent pending = new SagaOutboxEvent();
        pending.setEventId(UUID.randomUUID());
        pending.setAggregateId("940021");
        pending.setEventType(SagaManager.CMD_CREATE_DELIVERY);
        pending.setTopic(SagaManager.CMD_CREATE_DELIVERY);
        pending.setEventKey("940021");
        pending.setPayload("{\"orderId\":940021}");
        pending.setStatus(SagaOutboxEvent.Status.PENDING);
        pending.setAttempts(0);
        pending.setCreatedAt(LocalDateTime.now().minusSeconds(2));
        pending.setNextAttemptAt(LocalDateTime.now().minusSeconds(1));
        pending = outboxRepository.saveAndFlush(pending);

        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        CountDownLatch firstSendEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstSend = new CountDownLatch(1);
        AtomicInteger sends = new AtomicInteger();
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenAnswer(invocation -> {
            if (sends.incrementAndGet() == 1) {
                firstSendEntered.countDown();
                if (!releaseFirstSend.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Test did not release the first outbox relay");
                }
            }
            return CompletableFuture.completedFuture(null);
        });

        SagaOutboxRelay firstRelay = relay(kafkaTemplate);
        SagaOutboxRelay secondRelay = relay(kafkaTemplate);
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Throwable> first = executor.submit(() -> inTransaction(
                    transaction, firstRelay::relayCommands));
            assertThat(firstSendEntered.await(10, TimeUnit.SECONDS)).isTrue();

            // The first relay is still in its transaction and holds the row lock.
            // The second one must SKIP LOCKED rather than emitting the same event.
            Future<Throwable> second = executor.submit(() -> inTransaction(
                    transaction, secondRelay::relayCommands));
            assertThat(second.get(10, TimeUnit.SECONDS)).isNull();
            assertThat(sends.get()).isOne();

            releaseFirstSend.countDown();
            assertThat(first.get(10, TimeUnit.SECONDS)).isNull();
        } finally {
            releaseFirstSend.countDown();
            executor.shutdownNow();
        }

        assertThat(sends.get()).isOne();
        assertThat(outboxRepository.findById(pending.getId())).get()
                .extracting(SagaOutboxEvent::getStatus, SagaOutboxEvent::getAttempts)
                .containsExactly(SagaOutboxEvent.Status.SENT, 0);
    }

    @org.junit.jupiter.api.extension.ExtendWith(org.springframework.boot.test.system.OutputCaptureExtension.class)
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(value = SagaInstance.SagaStatus.class,
            names = {"OFFER_PERSISTING", "COMPENSATING", "OFFER_RETIRING"})
    void twoSchedulersResendSameCommandOnceAndExhaustBudget(SagaInstance.SagaStatus state,
            org.springframework.boot.test.system.CapturedOutput output) throws Exception {
        String topic = switch (state) {
            case OFFER_PERSISTING -> SagaManager.CMD_CACHE_SHIPPER_FOUND;
            case COMPENSATING -> SagaManager.CMD_CANCEL_DELIVERY;
            default -> SagaManager.CMD_EXPIRE_SHIPPER_OFFER;
        };
        var saga = new SagaInstance();
        saga.setSagaType("ORDER_CREATION");
        saga.setOrderId(950001L);
        saga.setStatus(state);
        saga.setStateEnteredAt(LocalDateTime.now().minusSeconds(121));
        saga = sagaRepository.saveAndFlush(saga);
        var command = new SagaOutboxEvent();
        command.setEventId(UUID.randomUUID());
        command.setAggregateId("950001");
        command.setEventType(topic);
        command.setTopic(topic);
        command.setEventKey("950001");
        String payload = "{\"orderId\":950001,\"eventId\":\"" + command.getEventId() + "\"}";
        command.setPayload(payload);
        command.setStatus(SagaOutboxEvent.Status.SENT);
        command.setCreatedAt(LocalDateTime.now().minusSeconds(122));
        command.setNextAttemptAt(command.getCreatedAt());
        command = outboxRepository.saveAndFlush(command);
        UUID eventId = command.getEventId();
        var firstScheduler = new SagaStuckStateTimeoutScheduler(sagaRepository, stuckTimeouts);
        var secondScheduler = new SagaStuckStateTimeoutScheduler(sagaRepository, stuckTimeouts);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> invokeTogether(ready, start, firstScheduler::checkTimeouts));
            var second = executor.submit(() -> invokeTogether(ready, start, secondScheduler::checkTimeouts));
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            assertThat(first.get(30, TimeUnit.SECONDS)).isNull();
            assertThat(second.get(30, TimeUnit.SECONDS)).isNull();
        } finally {
            executor.shutdownNow();
        }
        assertThat(sagaRepository.findById(saga.getId()).orElseThrow().getStuckResendAttempts()).isOne();
        assertThat(outboxRepository.count()).isOne();
        assertThat(outboxRepository.findById(command.getId()).orElseThrow())
                .extracting(SagaOutboxEvent::getEventId, SagaOutboxEvent::getPayload, SagaOutboxEvent::getStatus)
                .containsExactly(eventId, payload, SagaOutboxEvent.Status.PENDING);
        // Immediate repeated poll does not resend or prematurely fail.
        firstScheduler.checkTimeouts();
        assertThat(sagaRepository.findById(saga.getId()).orElseThrow().getStuckResendAttempts()).isOne();
        var tx = new TransactionTemplate(transactionManager);
        for (int attempt = 2; attempt <= 3; attempt++) {
            tx.executeWithoutResult(ignored -> {
                var locked = sagaRepository.findByOrderIdForUpdate(950001L).orElseThrow();
                locked.setStuckLastResentAt(LocalDateTime.now().minusSeconds(121));
                var sent = outboxRepository.findByIdForUpdate(outboxRepository.findAll().get(0).getId()).orElseThrow();
                sent.setStatus(SagaOutboxEvent.Status.SENT);
            });
            firstScheduler.checkTimeouts();
            assertThat(sagaRepository.findById(saga.getId()).orElseThrow().getStuckResendAttempts()).isEqualTo(attempt);
        }
        double before = meters.counter("dispatch.stuck.state.failed", "state", state.name()).count();
        tx.executeWithoutResult(ignored -> sagaRepository.findByOrderIdForUpdate(950001L).orElseThrow()
                .setStuckLastResentAt(LocalDateTime.now().minusSeconds(121)));
        firstScheduler.checkTimeouts();
        secondScheduler.checkTimeouts();
        assertThat(sagaRepository.findById(saga.getId()).orElseThrow().getStatus()).isEqualTo(SagaInstance.SagaStatus.FAILED);
        assertThat(meters.counter("dispatch.stuck.state.failed", "state", state.name()).count()).isEqualTo(before + 1);
        assertThat(outboxRepository.findById(command.getId()).orElseThrow().getEventId()).isEqualTo(eventId);
        assertThat(output.getAll()).contains("manual reconciliation required caseId=" + saga.getId())
                .contains("orderId=950001, state=" + state);
    }

    private SagaOutboxRelay relay(KafkaTemplate<String, Object> kafkaTemplate) {
        SagaOutboxRelay relay = new SagaOutboxRelay(outboxRepository, kafkaTemplate, new ObjectMapper());
        ReflectionTestUtils.setField(relay, "batchSize", 10);
        ReflectionTestUtils.setField(relay, "sendTimeoutSeconds", 5L);
        ReflectionTestUtils.setField(relay, "maxAttempts", 3);
        return relay;
    }

    private Throwable inTransaction(TransactionTemplate transaction, ThrowingOperation operation) {
        try {
            transaction.executeWithoutResult(ignored -> {
                try {
                    operation.run();
                } catch (Exception exception) {
                    throw new IllegalStateException(exception);
                }
            });
            return null;
        } catch (Throwable failure) {
            return failure;
        }
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
