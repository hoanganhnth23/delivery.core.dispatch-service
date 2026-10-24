package com.delivery.saga_orchestrator_service.listener;

import com.delivery.saga_orchestrator_service.SagaOrchestratorServiceApplication;
import com.delivery.saga_orchestrator_service.entity.SagaInstance;
import com.delivery.saga_orchestrator_service.entity.SagaOutboxEvent;
import com.delivery.saga_orchestrator_service.repository.SagaEarlyEventRepository;
import com.delivery.saga_orchestrator_service.repository.SagaInboundReceiptRepository;
import com.delivery.saga_orchestrator_service.repository.SagaInstanceRepository;
import com.delivery.saga_orchestrator_service.repository.SagaOutboxEventRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Rehearses the real Saga inbox boundary. Independent Saga contexts share a
 * Kafka group and PostgreSQL database: duplicate records from two partitions,
 * same/fresh-group raw replays and contradictory event-id reuse must converge
 * to one durable result, with only the contradiction reaching the owner DLT.
 */
@SpringBootTest(classes = SagaOrchestratorServiceApplication.class, properties = {
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false",
        "app.outbox.relay-enabled=false",
        "spring.task.scheduling.enabled=false",
        "spring.cloud.discovery.enabled=false",
        "eureka.client.enabled=false"
})
@Testcontainers(disabledWithoutDocker = true)
class SagaKafkaPostgresIntegrationTest {

    private static final String ORDER_CREATED_TOPIC = "order.created.saga-inbox-proof";
    private static final String ORDER_CANCELLED_TOPIC = "order.cancelled.saga-early-inbox-proof";
    private static final String DELIVERY_CREATED_TOPIC = "delivery.created.result.saga-inbox-proof";

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:3.8.0"));

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("saga_kafka")
            .withUsername("saga")
            .withPassword("saga");

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "validate");
        registry.add("spring.flyway.enabled", () -> "true");
        registry.add("spring.flyway.baseline-on-migrate", () -> "true");
    }

    @Autowired private SagaInstanceRepository sagaRepository;
    @Autowired private SagaInboundReceiptRepository receiptRepository;
    @Autowired private SagaEarlyEventRepository earlyEventRepository;
    @Autowired private SagaOutboxEventRepository outboxRepository;
    @Autowired @Qualifier("retryKafkaTemplate") private KafkaTemplate<String, String> rawKafka;

    private final List<ConfigurableApplicationContext> replicas = new ArrayList<>();

    @BeforeEach
    void prepareBoundary() {
        closeReplicas();
        outboxRepository.deleteAll();
        receiptRepository.deleteAll();
        earlyEventRepository.deleteAll();
        sagaRepository.deleteAll();
    }

    @AfterEach
    void stopReplicas() {
        closeReplicas();
    }

    @Test
    void orderCreatedKafkaPostgresReplayAndContradictoryReuseConvergeAcrossTwoSagaReplicas() throws Exception {
        UUID eventId = UUID.randomUUID();
        long orderId = 980_001L;
        String payload = orderCreated(eventId, orderId, "canonical");

        verifyReplayBoundary(
                ORDER_CREATED_TOPIC,
                "app.kafka.input-topics.order-created",
                Long.toString(orderId),
                payload,
                orderCreated(eventId, orderId, "contradictory"),
                () -> {
                    assertThat(receiptRepository.count()).isEqualTo(1);
                    assertThat(sagaRepository.findAll()).singleElement()
                            .extracting(SagaInstance::getStatus)
                            .isEqualTo(SagaInstance.SagaStatus.STARTED);
                    assertThat(outboxRepository.findAll())
                            .extracting(SagaOutboxEvent::getEventType)
                            .containsExactly("saga.command.create-delivery");
                });
    }

    @Test
    void earlyCancellationKafkaPostgresReplayAndContradictoryReuseConvergeAcrossTwoSagaReplicas() throws Exception {
        UUID eventId = UUID.randomUUID();
        long orderId = 980_002L;
        String payload = orderCancelled(eventId, orderId, "canonical");

        verifyReplayBoundary(
                ORDER_CANCELLED_TOPIC,
                "app.kafka.input-topics.order-cancelled",
                Long.toString(orderId),
                payload,
                orderCancelled(eventId, orderId, "contradictory"),
                () -> {
                    assertThat(sagaRepository.count()).isZero();
                    assertThat(receiptRepository.count()).isZero();
                    assertThat(earlyEventRepository.count()).isEqualTo(1);
                    assertThat(outboxRepository.count()).isZero();
                });
    }

    @Test
    void deliveryCreatedKafkaPostgresReplayAndContradictoryReuseConvergeAcrossTwoSagaReplicas() throws Exception {
        long orderId = 980_003L;
        long deliveryId = 980_004L;
        SagaInstance saga = new SagaInstance();
        saga.setSagaType("ORDER_CREATION");
        saga.setOrderId(orderId);
        saga.setStatus(SagaInstance.SagaStatus.STARTED);
        saga.setPayload(orderCreated(UUID.randomUUID(), orderId, "existing saga"));
        sagaRepository.saveAndFlush(saga);

        UUID eventId = UUID.randomUUID();
        String payload = deliveryCreated(eventId, orderId, deliveryId, "canonical");

        verifyReplayBoundary(
                DELIVERY_CREATED_TOPIC,
                "app.kafka.input-topics.delivery-created",
                Long.toString(orderId),
                payload,
                deliveryCreated(eventId, orderId, deliveryId, "contradictory"),
                () -> {
                    assertThat(receiptRepository.count()).isEqualTo(1);
                    assertThat(sagaRepository.findAll()).singleElement().satisfies(value -> {
                        assertThat(value.getStatus()).isEqualTo(SagaInstance.SagaStatus.DELIVERY_CREATED);
                        assertThat(value.getDeliveryId()).isEqualTo(deliveryId);
                    });
                    assertThat(outboxRepository.count()).isZero();
                });
    }

    private void verifyReplayBoundary(String topic, String property, String key,
                                      String payload, String contradictory,
                                      ThrowingRunnable invariant) throws Exception {
        createRequiredTopics(topic);
        String replicaGroup = "saga-inbox-replicas-" + UUID.randomUUID();
        String replayGroup = "saga-inbox-replay-" + UUID.randomUUID();

        startReplica(replicaGroup, property, topic);
        startReplica(replicaGroup, property, topic);
        await("two Saga replicas to own both source partitions for " + topic, () ->
                targetPartitionOwners(replicaGroup, topic).equals(Set.of(0, 1)));

        rawKafka.send(topic, 0, key, payload).get(10, TimeUnit.SECONDS);
        rawKafka.send(topic, 1, key, payload).get(10, TimeUnit.SECONDS);
        await("one Saga effect before both source offsets for " + topic, () -> {
            try {
                invariant.run();
                return committedOffsetsAtLeast(replicaGroup, topic, 1, 1);
            } catch (AssertionError | Exception ignored) {
                return false;
            }
        });

        rawKafka.send(topic, 0, key, payload).get(10, TimeUnit.SECONDS);
        await("same-group exact replay to commit as a no-op for " + topic, () ->
                committedOffsetsAtLeast(replicaGroup, topic, 2, 1));
        invariant.run();

        closeReplicas();
        startReplica(replayGroup, property, topic);
        await("fresh Saga group to replay historical offsets as no-ops for " + topic, () ->
                committedOffsetsAtLeast(replayGroup, topic, 2, 1));
        invariant.run();

        try (KafkaConsumer<String, String> dltConsumer = freshConsumer(topic + ".saga.DLT")) {
            rawKafka.send(topic, 1, key, contradictory).get(10, TimeUnit.SECONDS);
            ConsumerRecord<String, String> dlt = awaitRecord(
                    dltConsumer, topic + ".saga.DLT",
                    "contradictory Saga receipt reuse to reach the DLT for " + topic);
            assertThat(dlt.value()).isEqualTo(contradictory);
            await("contradictory source offset to be recovered after DLT publication for " + topic, () ->
                    committedOffsetsAtLeast(replayGroup, topic, 2, 2));
        }
        invariant.run();
    }

    private ConfigurableApplicationContext startReplica(String groupId, String topicProperty, String topic) {
        ConfigurableApplicationContext context = new SpringApplicationBuilder(SagaOrchestratorServiceApplication.class)
                .web(WebApplicationType.NONE)
                .properties(replicaProperties(groupId, topicProperty, topic))
                .run();
        listenerRegistry(context).getListenerContainers().stream()
                .filter(container -> listensTo(container, topic))
                .forEach(MessageListenerContainer::start);
        replicas.add(context);
        return context;
    }

    private Map<String, Object> replicaProperties(String groupId, String topicProperty, String topic) {
        Map<String, Object> properties = new java.util.HashMap<>();
        properties.put("spring.main.banner-mode", "off");
        properties.put("spring.kafka.bootstrap-servers", KAFKA.getBootstrapServers());
        properties.put("spring.kafka.consumer.group-id", groupId);
        properties.put("spring.kafka.listener.auto-startup", "false");
        properties.put("spring.kafka.admin.auto-create", "false");
        properties.put("spring.datasource.url", POSTGRES.getJdbcUrl());
        properties.put("spring.datasource.username", POSTGRES.getUsername());
        properties.put("spring.datasource.password", POSTGRES.getPassword());
        properties.put("spring.datasource.driver-class-name", "org.postgresql.Driver");
        properties.put("spring.jpa.hibernate.ddl-auto", "validate");
        properties.put("spring.flyway.enabled", "true");
        properties.put("spring.flyway.baseline-on-migrate", "true");
        properties.put(topicProperty, topic);
        properties.put("app.outbox.relay-enabled", "false");
        properties.put("spring.task.scheduling.enabled", "false");
        properties.put("spring.cloud.discovery.enabled", "false");
        properties.put("eureka.client.enabled", "false");
        return properties;
    }

    private KafkaListenerEndpointRegistry listenerRegistry(ConfigurableApplicationContext context) {
        return context.getBean(KafkaListenerEndpointRegistry.class);
    }

    private boolean listensTo(MessageListenerContainer container, String topic) {
        String[] topics = container.getContainerProperties().getTopics();
        return topics != null && Arrays.asList(topics).contains(topic);
    }

    private Set<Integer> targetPartitionOwners(String groupId, String topic) {
        try (AdminClient admin = adminClient()) {
            ConsumerGroupDescription group = admin.describeConsumerGroups(List.of(groupId)).all()
                    .get(10, TimeUnit.SECONDS).get(groupId);
            return group.members().stream()
                    .flatMap(member -> member.assignment().topicPartitions().stream())
                    .filter(partition -> topic.equals(partition.topic()))
                    .map(TopicPartition::partition)
                    .collect(java.util.stream.Collectors.toSet());
        } catch (Exception ignored) {
            return Set.of();
        }
    }

    private boolean committedOffsetsAtLeast(String groupId, String topic, long partitionZero, long partitionOne) {
        try (AdminClient admin = adminClient()) {
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets =
                    admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata()
                            .get(10, TimeUnit.SECONDS);
            return offsets.getOrDefault(new TopicPartition(topic, 0),
                    new org.apache.kafka.clients.consumer.OffsetAndMetadata(0)).offset() >= partitionZero
                    && offsets.getOrDefault(new TopicPartition(topic, 1),
                    new org.apache.kafka.clients.consumer.OffsetAndMetadata(0)).offset() >= partitionOne;
        } catch (Exception ignored) {
            return false;
        }
    }

    private void createRequiredTopics(String topic) throws Exception {
        try (AdminClient admin = adminClient()) {
            admin.createTopics(List.of(
                    new NewTopic(topic, 2, (short) 1),
                    new NewTopic(topic + ".saga.DLT", 2, (short) 1))).all().get(10, TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException exception) {
            if (!(exception.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException)) {
                throw exception;
            }
        }
    }

    private AdminClient adminClient() {
        return AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA.getBootstrapServers()));
    }

    private KafkaConsumer<String, String> freshConsumer(String topic) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "saga-inbox-dlt-proof-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private ConsumerRecord<String, String> awaitRecord(KafkaConsumer<String, String> consumer,
                                                         String topic, String description) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                if (topic.equals(record.topic())) {
                    return record;
                }
            }
        }
        fail("Timed out waiting for " + description);
        throw new AssertionError("unreachable");
    }

    private void await(String description, BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting for " + description, exception);
            }
        }
        fail("Timed out waiting for " + description);
    }

    private String orderCreated(UUID eventId, long orderId, String notes) {
        return "{\"eventId\":\"" + eventId + "\",\"orderId\":" + orderId
                + ",\"totalPrice\":120000,\"shippingFee\":15000,\"paymentMethod\":\"COD\""
                + ",\"restaurantId\":9,\"notes\":\"" + notes + "\"}";
    }

    private String orderCancelled(UUID eventId, long orderId, String reason) {
        return "{\"eventId\":\"" + eventId + "\",\"orderId\":" + orderId
                + ",\"reason\":\"" + reason + "\"}";
    }

    private String deliveryCreated(UUID eventId, long orderId, long deliveryId, String note) {
        return "{\"eventId\":\"" + eventId + "\",\"orderId\":" + orderId
                + ",\"deliveryId\":" + deliveryId + ",\"note\":\"" + note + "\"}";
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private void closeReplicas() {
        for (int index = replicas.size() - 1; index >= 0; index--) {
            replicas.get(index).close();
        }
        replicas.clear();
    }
}
