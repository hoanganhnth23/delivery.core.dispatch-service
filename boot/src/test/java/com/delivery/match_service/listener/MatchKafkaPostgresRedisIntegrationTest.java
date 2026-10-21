package com.delivery.match_service.listener;

import com.delivery.match_service.MatchServiceApplication;
import com.delivery.match_service.dto.event.FindShipperEvent;
import com.delivery.match_service.entity.MatchCommand;
import com.delivery.match_service.entity.MatchCancellationTombstone;
import com.delivery.match_service.entity.MatchOutboxEvent;
import com.delivery.match_service.repository.MatchCancellationTombstoneRepository;
import com.delivery.match_service.repository.MatchCommandRepository;
import com.delivery.match_service.repository.MatchOutboxEventRepository;
import com.delivery.match_service.repository.MatchRedisGeoRepository;
import com.delivery.match_service.service.MatchCancellationService;
import com.delivery.match_service.service.MatchCancellationProjectionRelay;
import com.delivery.match_service.service.MatchCommandStore;
import com.delivery.match_service.service.MatchOutboxRelay;
import com.delivery.match_service.service.SettlementEligibilityClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Exercises the real Match ingress boundary: separate Kafka topics, Redis
 * cancellation projection and PostgreSQL tombstone/command state. The test
 * deliberately seeds an eligible Redis shipper. If an old find command is not
 * fenced, it would reserve that shipper and create a durable result outbox.
 */
@SpringBootTest(classes = {
        MatchServiceApplication.class,
        MatchKafkaPostgresRedisIntegrationTest.IntegrationDependencies.class
}, properties = {
        "spring.kafka.listener.auto-startup=false",
        "match.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false",
        "app.outbox.relay-enabled=false",
        "spring.task.scheduling.enabled=false",
        "spring.cloud.discovery.enabled=false",
        "eureka.client.enabled=false"
})
@Testcontainers(disabledWithoutDocker = true)
class MatchKafkaPostgresRedisIntegrationTest {

    private static final String FIND_TOPIC = "saga.command.find-shipper";
    private static final String STOP_TOPIC = "saga.command.stop-matching";
    private static final String FOUND_TOPIC = "shipper.found";
    private static final String LOCATION_TOPIC = "shipper.location-updated";
    private static final String STATUS_TOPIC = "shipper.status-change";
    private static final String REPLAY_PROOF_TOPIC =
            "saga.command.find-shipper.match-inbox-proof";
    private static final long ORDER_ID = 456_000_001L;
    private static final long DELIVERY_ID = 123_000_001L;
    private static final long SHIPPER_ID = 789_000_001L;
    private static final UUID FIND_EVENT_ID =
            UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID STOP_EVENT_ID =
            UUID.fromString("22222222-2222-2222-2222-222222222222");
    private static final UUID MATCHING_SESSION_ID =
            UUID.fromString("33333333-3333-3333-3333-333333333333");
    private static final long HAPPY_ORDER_ID = 456_000_002L;
    private static final long HAPPY_DELIVERY_ID = 123_000_002L;
    private static final UUID HAPPY_FIND_EVENT_ID =
            UUID.fromString("44444444-4444-4444-4444-444444444444");
    private static final UUID HAPPY_MATCHING_SESSION_ID =
            UUID.fromString("55555555-5555-5555-5555-555555555555");
    private static final long CANCELLED_RESULT_ORDER_ID = 456_000_003L;
    private static final long CANCELLED_RESULT_DELIVERY_ID = 123_000_003L;
    private static final UUID CANCELLED_RESULT_FIND_EVENT_ID =
            UUID.fromString("66666666-6666-6666-6666-666666666666");
    private static final UUID CANCELLED_RESULT_STOP_EVENT_ID =
            UUID.fromString("77777777-7777-7777-7777-777777777777");
    private static final UUID CANCELLED_RESULT_SESSION_ID =
            UUID.fromString("88888888-8888-8888-8888-888888888888");
    private static final long REDIS_RECOVERY_ORDER_ID = 456_000_004L;
    private static final long REDIS_RECOVERY_DELIVERY_ID = 123_000_004L;
    private static final UUID REDIS_RECOVERY_FIND_EVENT_ID =
            UUID.fromString("99999999-9999-9999-9999-999999999999");
    private static final UUID REDIS_RECOVERY_SESSION_ID =
            UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static final long REDIS_RECOVERY_NEXT_ORDER_ID = 456_000_005L;
    private static final long REDIS_RECOVERY_NEXT_DELIVERY_ID = 123_000_005L;
    private static final UUID REDIS_RECOVERY_NEXT_FIND_EVENT_ID =
            UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
    private static final UUID REDIS_RECOVERY_NEXT_SESSION_ID =
            UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:3.8.0"));

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("match_kafka_redis")
            .withUsername("match")
            .withPassword("match");

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "validate");
        registry.add("spring.flyway.enabled", () -> "true");
        registry.add("spring.flyway.baseline-on-migrate", () -> "true");
    }

    @Autowired private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired private KafkaListenerEndpointRegistry listenerRegistry;
    @Autowired private MatchCommandRepository commandRepository;
    @Autowired private MatchCancellationTombstoneRepository tombstoneRepository;
    @Autowired private MatchOutboxEventRepository outboxRepository;
    @Autowired private MatchRedisGeoRepository redisGeoRepository;
    @Autowired private MatchCancellationService cancellationService;
    @Autowired private MatchCancellationProjectionRelay cancellationProjectionRelay;
    @Autowired private MatchCommandStore matchCommandStore;
    @Autowired private RedisTemplate<String, Object> redisTemplate;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private PlatformTransactionManager transactionManager;

    private final List<ConfigurableApplicationContext> replicas = new ArrayList<>();

    @BeforeEach
    void prepareBoundary() throws Exception {
        closeReplicas();
        stopListenersFor(FIND_TOPIC);
        stopListenersFor(STOP_TOPIC);
        stopListenersFor(LOCATION_TOPIC);
        stopListenersFor(STATUS_TOPIC);
        createRequiredTopics();
        outboxRepository.deleteAll();
        commandRepository.deleteAll();
        tombstoneRepository.deleteAll();
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushDb();
    }

    @AfterEach
    void stopBoundaryListeners() {
        closeReplicas();
        stopListenersFor(FIND_TOPIC);
        stopListenersFor(STOP_TOPIC);
        stopListenersFor(LOCATION_TOPIC);
        stopListenersFor(STATUS_TOPIC);
    }

    @Test
    void delayedOlderLocationFactsCannotUndoNewerOnlineOrOfflineDespiteKafkaPublicationOrder() throws Exception {
        startListenersFor(LOCATION_TOPIC);
        long observed = System.currentTimeMillis() - 5000;
        kafkaTemplate.send(LOCATION_TOPIC, Long.toString(SHIPPER_ID), locationPayload(observed)).get();
        var oldOffline = objectMapper.createObjectNode();
        oldOffline.put("shipperId", SHIPPER_ID); oldOffline.put("isOnline", false);
        oldOffline.put("timestamp", observed - 1);
        var delayedOffline = kafkaTemplate.send(LOCATION_TOPIC, Long.toString(SHIPPER_ID),
                objectMapper.writeValueAsString(oldOffline)).get();
        await("both new online and delayed older offline to commit", () -> committedOffsetAtLeast(
                LOCATION_TOPIC, delayedOffline.getRecordMetadata().offset() + 1));
        assertThat(redisTemplate.opsForSet().isMember("match:shippers:online", Long.toString(SHIPPER_ID))).isTrue();
        assertThat(redisTemplate.opsForGeo().position("match:shippers:geo", Long.toString(SHIPPER_ID)).get(0)).isNotNull();

        oldOffline.put("timestamp", observed + 1);
        kafkaTemplate.send(LOCATION_TOPIC, Long.toString(SHIPPER_ID), objectMapper.writeValueAsString(oldOffline)).get();
        var delayedOnline = kafkaTemplate.send(LOCATION_TOPIC, Long.toString(SHIPPER_ID), locationPayload(observed - 2)).get();
        await("both new offline and delayed older online to commit", () -> committedOffsetAtLeast(
                LOCATION_TOPIC, delayedOnline.getRecordMetadata().offset() + 1));
        assertThat(redisTemplate.opsForSet().isMember("match:shippers:online", Long.toString(SHIPPER_ID))).isFalse();
        assertThat(redisTemplate.opsForGeo().position("match:shippers:geo", Long.toString(SHIPPER_ID)).get(0)).isNull();
    }

    @Test
    void stopBeforeFindIsDurablyFencedAcrossKafkaPostgresAndRedis() throws Exception {
        // If find were allowed to run, this real eligible candidate would be
        // reserved in Redis and a shipper.found outbox event would be stored.
        redisGeoRepository.addOrUpdateShipperLocation(
                SHIPPER_ID, 10.762622, 106.660172, true, System.currentTimeMillis());

        startListenersFor(STOP_TOPIC);
        kafkaTemplate.send(STOP_TOPIC, Long.toString(DELIVERY_ID), stopPayload()).get();

        await("Kafka stop command persisted in PostgreSQL and Redis", () ->
                tombstoneRepository.findById(STOP_EVENT_ID).isPresent()
                        && cancellationService.isCancelled(DELIVERY_ID, MATCHING_SESSION_ID));

        startListenersFor(FIND_TOPIC);
        kafkaTemplate.send(FIND_TOPIC, Long.toString(DELIVERY_ID),
                objectMapper.writeValueAsString(findCommand())).get();

        await("Kafka find command terminally fenced by the prior stop", () ->
                commandRepository.findById(FIND_EVENT_ID)
                        .map(command -> command.getStatus() == MatchCommand.Status.CANCELLED)
                        .orElse(false));

        assertThat(tombstoneRepository.findById(STOP_EVENT_ID)).isPresent();
        assertThat(commandRepository.findById(FIND_EVENT_ID)).get()
                .extracting(MatchCommand::getStatus)
                .isEqualTo(MatchCommand.Status.CANCELLED);
        assertThat(cancellationService.isCancelled(DELIVERY_ID, MATCHING_SESSION_ID)).isTrue();
        assertThat(redisTemplate.hasKey(cancellationKey())).isTrue();
        assertThat(redisTemplate.hasKey("match:shipper:offer:" + SHIPPER_ID)).isFalse();
        assertThat(redisTemplate.hasKey("match:delivery:offer:" + DELIVERY_ID)).isFalse();
        assertThat(outboxRepository.count()).isZero();
    }

    @Test
    void kafkaFindCreatesOneRedisOfferAndOneDeterministicResultThenRelaysItAcrossReplay() throws Exception {
        FindShipperEvent command = findCommand(
                HAPPY_FIND_EVENT_ID, HAPPY_MATCHING_SESSION_ID, HAPPY_ORDER_ID, HAPPY_DELIVERY_ID);
        redisGeoRepository.addOrUpdateShipperLocation(
                SHIPPER_ID, 10.762622, 106.660172, true, System.currentTimeMillis());
        startListenersFor(FIND_TOPIC);

        String payload = objectMapper.writeValueAsString(command);
        SendResult<String, String> firstDelivery = kafkaTemplate.send(
                FIND_TOPIC, Long.toString(HAPPY_DELIVERY_ID), payload).get();

        await("Kafka find command persisted its offer and outbox result", () ->
                commandRepository.findById(HAPPY_FIND_EVENT_ID)
                        .map(persisted -> persisted.getStatus() == MatchCommand.Status.RESULT_STAGED)
                        .orElse(false)
                        && businessResults().size() == 1);

        UUID expectedResultId = UUID.nameUUIDFromBytes(
                ("match:shipper-found:" + HAPPY_FIND_EVENT_ID).getBytes(StandardCharsets.UTF_8));
        MatchOutboxEvent result = businessResults().get(0);
        assertThat(result.getStatus()).isEqualTo(MatchOutboxEvent.Status.PENDING);
        assertThat(result.getCommandEventId()).isEqualTo(HAPPY_FIND_EVENT_ID);
        assertThat(result.getEventId()).isEqualTo(expectedResultId);
        assertThat(result.getTopic()).isEqualTo("shipper.found");
        assertThat(redisTemplate.opsForValue().get("match:shipper:offer:" + SHIPPER_ID))
                .isEqualTo(Long.toString(HAPPY_DELIVERY_ID));
        assertThat(redisTemplate.opsForValue().get("match:delivery:offer:" + HAPPY_DELIVERY_ID))
                .isEqualTo(Long.toString(SHIPPER_ID));

        SendResult<String, String> replayDelivery = kafkaTemplate.send(
                FIND_TOPIC, Long.toString(HAPPY_DELIVERY_ID), payload).get();
        await("the exact replay to be acknowledged by the real Match consumer", () ->
                committedOffsetAtLeast(FIND_TOPIC, replayDelivery.getRecordMetadata().offset() + 1));

        assertThat(firstDelivery.getRecordMetadata().offset())
                .isLessThan(replayDelivery.getRecordMetadata().offset());
        assertThat(commandRepository.findById(HAPPY_FIND_EVENT_ID)).get()
                .extracting(MatchCommand::getStatus)
                .isEqualTo(MatchCommand.Status.RESULT_STAGED);
        assertThat(businessResults()).singleElement()
                .extracting(MatchOutboxEvent::getEventId)
                .isEqualTo(expectedResultId);

        try (KafkaConsumer<String, String> resultConsumer = freshConsumer(FOUND_TOPIC)) {
            relayPendingResults();
            ConsumerRecord<String, String> published = awaitRecord(
                    resultConsumer, FOUND_TOPIC, "durable Match outbox result", expectedResultId);
            assertThat(published.key()).isEqualTo(Long.toString(HAPPY_ORDER_ID));
            assertThat(objectMapper.readTree(published.value()).path("eventId").asText())
                    .isEqualTo(expectedResultId.toString());
            assertThat(objectMapper.readTree(published.value()).path("matchingSessionId").asText())
                    .isEqualTo(HAPPY_MATCHING_SESSION_ID.toString());
        }

        assertThat(businessResults()).singleElement()
                .extracting(MatchOutboxEvent::getStatus)
                .isEqualTo(MatchOutboxEvent.Status.SENT);
    }

    @Test
    void kafkaFindReplayAndContradictoryReuseConvergeAcrossTwoMatchReplicas() throws Exception {
        FindShipperEvent command = findCommand(
                UUID.fromString("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeee1"),
                UUID.fromString("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeee2"),
                456_000_006L,
                123_000_006L);
        FindShipperEvent contradictory = findCommand(
                command.getEventId(), command.getMatchingSessionId(),
                command.getOrderId(), command.getDeliveryId());
        contradictory.setDeliveryAddress("Contradictory replay address");

        createReplayProofTopics(REPLAY_PROOF_TOPIC);
        redisGeoRepository.addOrUpdateShipperLocation(
                SHIPPER_ID, 10.762622, 106.660172, true, System.currentTimeMillis());

        String replicaGroup = "match-find-replicas-" + UUID.randomUUID();
        String replayGroup = "match-find-replay-" + UUID.randomUUID();
        String payload = objectMapper.writeValueAsString(command);
        String contradictoryPayload = objectMapper.writeValueAsString(contradictory);

        startReplica(replicaGroup, REPLAY_PROOF_TOPIC);
        startReplica(replicaGroup, REPLAY_PROOF_TOPIC);
        await("both source partitions to be assigned within the Match replica group", () ->
                sourcePartitionOwnerCount(replicaGroup, REPLAY_PROOF_TOPIC) == 2);

        kafkaTemplate.send(REPLAY_PROOF_TOPIC, 0, Long.toString(command.getDeliveryId()), payload).get();
        kafkaTemplate.send(REPLAY_PROOF_TOPIC, 1, Long.toString(command.getDeliveryId()), payload).get();
        await("duplicate Match find commands to converge on one durable result", () ->
                oneFoundResult(command)
                        && committedOffsetsAtLeast(replicaGroup, REPLAY_PROOF_TOPIC, 1, 1));

        kafkaTemplate.send(REPLAY_PROOF_TOPIC, 0, Long.toString(command.getDeliveryId()), payload).get();
        await("same-group Match replay to commit as a no-op", () ->
                committedOffsetsAtLeast(replicaGroup, REPLAY_PROOF_TOPIC, 2, 1));
        assertOneFoundResult(command);

        closeReplicas();
        startReplica(replayGroup, REPLAY_PROOF_TOPIC);
        await("fresh Match group to replay historical records as no-ops", () ->
                committedOffsetsAtLeast(replayGroup, REPLAY_PROOF_TOPIC, 2, 1));
        assertOneFoundResult(command);

        try (KafkaConsumer<String, String> dltConsumer = freshConsumer(REPLAY_PROOF_TOPIC + ".DLT")) {
            kafkaTemplate.send(REPLAY_PROOF_TOPIC, 1, Long.toString(command.getDeliveryId()),
                    contradictoryPayload).get();
            ConsumerRecord<String, String> dlt = awaitRecord(
                    dltConsumer, REPLAY_PROOF_TOPIC + ".DLT",
                    "contradictory Match command identity reuse to reach the owner DLT",
                    command.getEventId());
            assertThat(dlt.value()).isEqualTo(contradictoryPayload);
        }
        await("contradictory Match source offset to commit after DLT recovery", () ->
                committedOffsetsAtLeast(replayGroup, REPLAY_PROOF_TOPIC, 2, 2));
        assertOneFoundResult(command);
    }

    @Test
    void redisProjectionLossRebuildsFromKafkaWithoutRecomputingDurableResult() throws Exception {
        FindShipperEvent command = findCommand(
                REDIS_RECOVERY_FIND_EVENT_ID,
                REDIS_RECOVERY_SESSION_ID,
                REDIS_RECOVERY_ORDER_ID,
                REDIS_RECOVERY_DELIVERY_ID);
        redisGeoRepository.addOrUpdateShipperLocation(
                SHIPPER_ID, 10.762622, 106.660172, true, System.currentTimeMillis());
        startListenersFor(FIND_TOPIC);

        String payload = objectMapper.writeValueAsString(command);
        kafkaTemplate.send(FIND_TOPIC, Long.toString(REDIS_RECOVERY_DELIVERY_ID), payload).get();
        await("initial Match result to be durable before Redis projection loss", () ->
                commandRepository.findById(REDIS_RECOVERY_FIND_EVENT_ID)
                        .map(persisted -> persisted.getStatus() == MatchCommand.Status.RESULT_STAGED)
                        .orElse(false)
                        && businessResults().size() == 1);

        MatchOutboxEvent durableResult = businessResults().get(0);
        UUID expectedResultId = UUID.nameUUIDFromBytes(
                ("match:shipper-found:" + REDIS_RECOVERY_FIND_EVENT_ID)
                        .getBytes(StandardCharsets.UTF_8));
        assertThat(durableResult.getEventId()).isEqualTo(expectedResultId);
        assertThat(redisTemplate.hasKey("match:shipper:offer:" + SHIPPER_ID)).isTrue();

        // Recovery intentionally discards Redis GEO/offer state rather than
        // restoring potentially stale realtime data from a backup.
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushDb();
        assertThat(redisTemplate.hasKey("match:shipper:offer:" + SHIPPER_ID)).isFalse();

        SendResult<String, String> replay = kafkaTemplate.send(
                FIND_TOPIC, Long.toString(REDIS_RECOVERY_DELIVERY_ID), payload).get();
        await("exact find replay after Redis loss to be acknowledged", () ->
                committedOffsetAtLeast(FIND_TOPIC, replay.getRecordMetadata().offset() + 1));

        assertThat(commandRepository.findById(REDIS_RECOVERY_FIND_EVENT_ID)).get()
                .extracting(MatchCommand::getStatus)
                .isEqualTo(MatchCommand.Status.RESULT_STAGED);
        assertThat(businessResults()).singleElement()
                .extracting(MatchOutboxEvent::getEventId, MatchOutboxEvent::getStatus)
                .containsExactly(expectedResultId, MatchOutboxEvent.Status.PENDING);
        assertThat(redisTemplate.hasKey("match:shipper:offer:" + SHIPPER_ID)).isFalse();

        relayPendingResults();
        assertThat(businessResults()).singleElement()
                .extracting(MatchOutboxEvent::getStatus)
                .isEqualTo(MatchOutboxEvent.Status.SENT);

        // A fresh tracking event rebuilds the volatile replica. Once Delivery
        // republishes BUSY for the recovered first result, Match must not offer
        // that shipper to the next command.
        startListenersFor(LOCATION_TOPIC);
        kafkaTemplate.send(LOCATION_TOPIC, Long.toString(SHIPPER_ID),
                locationPayload(System.currentTimeMillis())).get();
        await("fresh Kafka location to rebuild the Redis projection", () ->
                redisGeoRepository.findNearbyShippers(10.762622, 106.660172, 10.0, 10)
                        .stream().anyMatch(shipper -> shipper.shipperId.equals(SHIPPER_ID)));

        startListenersFor(STATUS_TOPIC);
        kafkaTemplate.send(STATUS_TOPIC, Long.toString(SHIPPER_ID),
                busyStatusPayload(System.currentTimeMillis())).get();
        await("recovered delivery status to fence the rebuilt shipper replica", () ->
                Boolean.TRUE.equals(redisTemplate.hasKey("match:shipper:busy:" + SHIPPER_ID)));

        FindShipperEvent nextCommand = findCommand(
                REDIS_RECOVERY_NEXT_FIND_EVENT_ID,
                REDIS_RECOVERY_NEXT_SESSION_ID,
                REDIS_RECOVERY_NEXT_ORDER_ID,
                REDIS_RECOVERY_NEXT_DELIVERY_ID);
        kafkaTemplate.send(FIND_TOPIC, Long.toString(REDIS_RECOVERY_NEXT_DELIVERY_ID),
                objectMapper.writeValueAsString(nextCommand)).get();
        await("next command to settle without re-offering the recovered busy shipper", () ->
                commandRepository.findById(REDIS_RECOVERY_NEXT_FIND_EVENT_ID)
                        .map(persisted -> persisted.getStatus() == MatchCommand.Status.RESULT_STAGED)
                        .orElse(false)
                        && businessResults().size() == 2);

        MatchOutboxEvent nextResult = businessResults().stream()
                .filter(event -> REDIS_RECOVERY_NEXT_FIND_EVENT_ID.equals(event.getCommandEventId()))
                .findFirst()
                .orElseThrow();
        assertThat(nextResult.getTopic()).isEqualTo("shipper.not-found");
        assertThat(redisTemplate.hasKey("match:shipper:offer:" + SHIPPER_ID)).isFalse();
        assertThat(redisTemplate.hasKey("match:delivery:offer:" + REDIS_RECOVERY_NEXT_DELIVERY_ID))
                .isFalse();
    }

    @Test
    void stopAfterResultStagingCancelsUnsentOutboxBeforeItCanReachKafka() throws Exception {
        FindShipperEvent command = findCommand(
                CANCELLED_RESULT_FIND_EVENT_ID,
                CANCELLED_RESULT_SESSION_ID,
                CANCELLED_RESULT_ORDER_ID,
                CANCELLED_RESULT_DELIVERY_ID);
        redisGeoRepository.addOrUpdateShipperLocation(
                SHIPPER_ID, 10.762622, 106.660172, true, System.currentTimeMillis());
        startListenersFor(FIND_TOPIC);
        kafkaTemplate.send(FIND_TOPIC, Long.toString(CANCELLED_RESULT_DELIVERY_ID),
                objectMapper.writeValueAsString(command)).get();

        await("Match result staged before the stop arrives", () ->
                commandRepository.findById(CANCELLED_RESULT_FIND_EVENT_ID)
                        .map(persisted -> persisted.getStatus() == MatchCommand.Status.RESULT_STAGED)
                        .orElse(false)
                        && businessResults().size() == 1);
        long outputEndBeforeStop = endOffset(FOUND_TOPIC);

        startListenersFor(STOP_TOPIC);
        kafkaTemplate.send(STOP_TOPIC, Long.toString(CANCELLED_RESULT_DELIVERY_ID),
                stopPayload(
                        CANCELLED_RESULT_STOP_EVENT_ID,
                        CANCELLED_RESULT_ORDER_ID,
                        CANCELLED_RESULT_DELIVERY_ID,
                        CANCELLED_RESULT_SESSION_ID)).get();

        await("stop to cancel the durable unsent Match result", () ->
                commandRepository.findById(CANCELLED_RESULT_FIND_EVENT_ID)
                        .map(persisted -> persisted.getStatus() == MatchCommand.Status.CANCELLED)
                        .orElse(false)
                        && outboxRepository.findAll().stream()
                                .allMatch(event -> event.getStatus() == MatchOutboxEvent.Status.CANCELLED)
                        && cancellationService.isCancelled(
                                CANCELLED_RESULT_DELIVERY_ID, CANCELLED_RESULT_SESSION_ID));

        relayPendingResults();

        assertThat(businessResults()).singleElement()
                .extracting(MatchOutboxEvent::getStatus)
                .isEqualTo(MatchOutboxEvent.Status.CANCELLED);
        assertThat(redisTemplate.hasKey("match:shipper:offer:" + SHIPPER_ID)).isFalse();
        assertThat(redisTemplate.hasKey("match:delivery:offer:" + CANCELLED_RESULT_DELIVERY_ID)).isFalse();
        assertThat(endOffset(FOUND_TOPIC)).isEqualTo(outputEndBeforeStop);
    }

    @Test
    void pendingCancellationProjectionIsClaimedFromPostgresAndWrittenToRedis() throws Exception {
        // The scheduler's claim query uses PostgreSQL's SKIP LOCKED boundary.
        // Keep this independent from Kafka so it precisely proves that a
        // committed fence survives until the Redis projection succeeds.
        matchCommandStore.recordStopMatching(
                STOP_EVENT_ID, ORDER_ID, DELIVERY_ID, MATCHING_SESSION_ID, stopPayload());

        assertThat(tombstoneRepository.findById(STOP_EVENT_ID)).get()
                .extracting(MatchCancellationTombstone::getProjectionStatus)
                .isEqualTo(MatchCancellationTombstone.ProjectionStatus.PENDING);

        cancellationProjectionRelay.relayPending();

        assertThat(tombstoneRepository.findById(STOP_EVENT_ID)).get()
                .extracting(MatchCancellationTombstone::getProjectionStatus)
                .isEqualTo(MatchCancellationTombstone.ProjectionStatus.PROJECTED);
        assertThat(cancellationService.isCancelled(DELIVERY_ID, MATCHING_SESSION_ID)).isTrue();
        assertThat(redisTemplate.hasKey(cancellationKey())).isTrue();
    }

    private void createRequiredTopics() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            List<NewTopic> topics = List.of(
                    new NewTopic(FIND_TOPIC, 1, (short) 1),
                    new NewTopic(STOP_TOPIC, 1, (short) 1),
                    new NewTopic(FOUND_TOPIC, 1, (short) 1),
                    new NewTopic(LOCATION_TOPIC, 1, (short) 1),
                    new NewTopic(STATUS_TOPIC, 1, (short) 1));
            admin.createTopics(topics).all().get();
        } catch (java.util.concurrent.ExecutionException exception) {
            Throwable cause = exception.getCause();
            if (!(cause instanceof org.apache.kafka.common.errors.TopicExistsException)) {
                throw exception;
            }
        }
    }

    private void createReplayProofTopics(String sourceTopic) throws Exception {
        List<NewTopic> topics = new ArrayList<>();
        topics.add(new NewTopic(sourceTopic, 2, (short) 1));
        for (long delay : List.of(1000L, 2000L, 4000L)) {
            topics.add(new NewTopic(sourceTopic + ".retry-" + delay, 2, (short) 1));
        }
        topics.add(new NewTopic(sourceTopic + ".DLT", 2, (short) 1));
        try (AdminClient admin = adminClient()) {
            admin.createTopics(topics).all().get(10, TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException exception) {
            if (!(exception.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException)) {
                throw exception;
            }
        }
    }

    private void startListenersFor(String topic) {
        List<MessageListenerContainer> containers = listenersFor(topic);
        assertThat(containers)
                .as("Kafka listener container for topic %s", topic)
                .isNotEmpty();
        containers.forEach(container -> {
            if (!container.isRunning()) {
                container.start();
            }
        });
    }

    private void stopListenersFor(String topic) {
        listenersFor(topic).forEach(container -> {
            if (container.isRunning()) {
                container.stop();
            }
        });
    }

    private List<MessageListenerContainer> listenersFor(String topic) {
        return listenerRegistry.getListenerContainers().stream()
                .filter(container -> {
                    String[] topics = container.getContainerProperties().getTopics();
                    return topics != null && Arrays.asList(topics).contains(topic);
                })
                .toList();
    }

    private ConfigurableApplicationContext startReplica(String groupId, String sourceTopic) {
        ConfigurableApplicationContext context = new SpringApplicationBuilder(
                MatchServiceApplication.class, IntegrationDependencies.class)
                .web(WebApplicationType.NONE)
                // @SpringBootTest's DynamicPropertySource is scoped to the
                // primary context. Independently booted replicas must receive
                // the Testcontainers endpoints directly instead of inheriting
                // test/resources' H2 defaults.
                .initializers((ApplicationContextInitializer<GenericApplicationContext>) applicationContext ->
                        applicationContext.getEnvironment().getPropertySources().addFirst(
                                new org.springframework.core.env.MapPropertySource(
                                        "match-replica-testcontainers", replicaProperties(groupId, sourceTopic))))
                .properties(replicaProperties(groupId, sourceTopic))
                .run();
        List<String> topics = new ArrayList<>();
        topics.add(sourceTopic);
        topics.add(sourceTopic + ".retry-1000");
        topics.add(sourceTopic + ".retry-2000");
        topics.add(sourceTopic + ".retry-4000");
        listenerRegistry(context).getListenerContainers().stream()
                .filter(container -> topics.stream().anyMatch(topic -> listensTo(container, topic)))
                .forEach(MessageListenerContainer::start);
        replicas.add(context);
        return context;
    }

    private Map<String, Object> replicaProperties(String groupId, String sourceTopic) {
        Map<String, Object> properties = new java.util.HashMap<>();
        properties.put("spring.main.banner-mode", "off");
        properties.put("spring.kafka.bootstrap-servers", KAFKA.getBootstrapServers());
        properties.put("spring.kafka.consumer.group-id", groupId);
        properties.put("spring.kafka.listener.auto-startup", "false");
        properties.put("match.kafka.listener.auto-startup", "false");
        properties.put("match.kafka.find-listener.auto-startup", "false");
        properties.put("spring.kafka.admin.auto-create", "false");
        properties.put("app.kafka.topics.find-shipper", sourceTopic);
        properties.put("spring.data.redis.host", REDIS.getHost());
        properties.put("spring.data.redis.port", REDIS.getMappedPort(6379));
        properties.put("spring.datasource.url", POSTGRES.getJdbcUrl());
        properties.put("spring.datasource.username", POSTGRES.getUsername());
        properties.put("spring.datasource.password", POSTGRES.getPassword());
        properties.put("spring.datasource.driver-class-name", "org.postgresql.Driver");
        properties.put("spring.jpa.hibernate.ddl-auto", "validate");
        properties.put("spring.flyway.enabled", "true");
        properties.put("spring.flyway.baseline-on-migrate", "true");
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

    private int sourcePartitionOwnerCount(String groupId, String topic) {
        try (AdminClient admin = adminClient()) {
            return (int) admin.describeConsumerGroups(List.of(groupId)).all().get(2, TimeUnit.SECONDS)
                    .get(groupId).members().stream()
                    .flatMap(member -> member.assignment().topicPartitions().stream())
                    .filter(partition -> topic.equals(partition.topic()))
                    .count();
        } catch (Exception ignored) {
            return 0;
        }
    }

    private boolean committedOffsetsAtLeast(
            String groupId, String topic, long partitionZero, long partitionOne) {
        try (AdminClient admin = adminClient()) {
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets =
                    admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata()
                            .get(2, TimeUnit.SECONDS);
            return offsets.getOrDefault(new TopicPartition(topic, 0),
                    new org.apache.kafka.clients.consumer.OffsetAndMetadata(0)).offset() >= partitionZero
                    && offsets.getOrDefault(new TopicPartition(topic, 1),
                    new org.apache.kafka.clients.consumer.OffsetAndMetadata(0)).offset() >= partitionOne;
        } catch (Exception ignored) {
            return false;
        }
    }

    private boolean oneFoundResult(FindShipperEvent command) {
        return commandRepository.findById(command.getEventId())
                .map(persisted -> persisted.getStatus() == MatchCommand.Status.RESULT_STAGED)
                .orElse(false)
                && businessResults().size() == 1
                && Objects.equals(redisTemplate.opsForValue().get("match:shipper:offer:" + SHIPPER_ID),
                        Long.toString(command.getDeliveryId()))
                && Objects.equals(redisTemplate.opsForValue().get(
                        "match:delivery:offer:" + command.getDeliveryId()), Long.toString(SHIPPER_ID));
    }

    private void assertOneFoundResult(FindShipperEvent command) {
        UUID expectedResultId = UUID.nameUUIDFromBytes(
                ("match:shipper-found:" + command.getEventId()).getBytes(StandardCharsets.UTF_8));
        assertThat(commandRepository.findById(command.getEventId())).get()
                .extracting(MatchCommand::getStatus)
                .isEqualTo(MatchCommand.Status.RESULT_STAGED);
        assertThat(businessResults()).singleElement().satisfies(outbox -> {
            assertThat(outbox.getCommandEventId()).isEqualTo(command.getEventId());
            assertThat(outbox.getEventId()).isEqualTo(expectedResultId);
            assertThat(outbox.getTopic()).isEqualTo("shipper.found");
        });
        assertThat(redisTemplate.opsForValue().get("match:shipper:offer:" + SHIPPER_ID))
                .isEqualTo(Long.toString(command.getDeliveryId()));
        assertThat(redisTemplate.opsForValue().get("match:delivery:offer:" + command.getDeliveryId()))
                .isEqualTo(Long.toString(SHIPPER_ID));
    }

    private AdminClient adminClient() {
        return AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA.getBootstrapServers()));
    }

    private List<MatchOutboxEvent> businessResults() {
        return outboxRepository.findAll().stream()
                .filter(outbox -> "shipper.found".equals(outbox.getTopic())
                        || "shipper.not-found".equals(outbox.getTopic()))
                .toList();
    }

    private void closeReplicas() {
        for (int index = replicas.size() - 1; index >= 0; index--) {
            replicas.get(index).close();
        }
        replicas.clear();
    }

    private FindShipperEvent findCommand() {
        return findCommand(FIND_EVENT_ID, MATCHING_SESSION_ID, ORDER_ID, DELIVERY_ID);
    }

    private FindShipperEvent findCommand(
            UUID eventId,
            UUID matchingSessionId,
            long orderId,
            long deliveryId) {
        FindShipperEvent event = new FindShipperEvent();
        event.setEventId(eventId);
        event.setMatchingSessionId(matchingSessionId);
        event.setOrderId(orderId);
        event.setDeliveryId(deliveryId);
        event.setRestaurantName("Kafka integration restaurant");
        event.setPickupAddress("1 Kafka Street");
        event.setPickupLat(10.762622);
        event.setPickupLng(106.660172);
        event.setDeliveryAddress("2 PostgreSQL Street");
        event.setDeliveryLat(10.775000);
        event.setDeliveryLng(106.700000);
        event.setTotalPrice(new BigDecimal("120000"));
        event.setPaymentMethod("COD");
        event.setMaxRetryAttempts(1);
        event.setInitialDelaySeconds(1);
        event.setMaxDelaySeconds(1);
        event.setBackoffMultiplier(1.0);
        event.setMatchingDeadlineAt(LocalDateTime.now().plusMinutes(5));
        return event;
    }

    private boolean committedOffsetAtLeast(String topic, long expectedExclusiveOffset) {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            var offsets = admin.listConsumerGroupOffsets("match-service")
                    .partitionsToOffsetAndMetadata().get();
            var offset = offsets.get(new TopicPartition(topic, 0));
            return offset != null && offset.offset() >= expectedExclusiveOffset;
        } catch (Exception ignored) {
            return false;
        }
    }

    private String stopPayload() throws Exception {
        return stopPayload(STOP_EVENT_ID, ORDER_ID, DELIVERY_ID, MATCHING_SESSION_ID);
    }

    private String stopPayload(
            UUID eventId,
            long orderId,
            long deliveryId,
            UUID matchingSessionId) throws Exception {
        var payload = objectMapper.createObjectNode();
        payload.put("eventId", eventId.toString());
        payload.put("orderId", orderId);
        payload.put("deliveryId", deliveryId);
        payload.put("matchingSessionId", matchingSessionId.toString());
        return objectMapper.writeValueAsString(payload);
    }

    private String locationPayload(long timestamp) throws Exception {
        var payload = objectMapper.createObjectNode();
        payload.put("shipperId", SHIPPER_ID);
        payload.put("latitude", 10.762622);
        payload.put("longitude", 106.660172);
        payload.put("isOnline", true);
        payload.put("timestamp", timestamp);
        return objectMapper.writeValueAsString(payload);
    }

    private String busyStatusPayload(long timestamp) throws Exception {
        var payload = objectMapper.createObjectNode();
        payload.put("eventId", "dddddddd-dddd-dddd-dddd-dddddddddddd");
        payload.put("shipperId", SHIPPER_ID);
        payload.put("orderId", REDIS_RECOVERY_ORDER_ID);
        payload.put("deliveryId", REDIS_RECOVERY_DELIVERY_ID);
        payload.put("status", "BUSY");
        payload.put("timestamp", timestamp);
        return objectMapper.writeValueAsString(payload);
    }

    private String cancellationKey() {
        return "match:cancelled:" + DELIVERY_ID + ":" + MATCHING_SESSION_ID;
    }

    private void relayPendingResults() {
        MatchOutboxRelay relay = new MatchOutboxRelay(outboxRepository, kafkaTemplate);
        ReflectionTestUtils.setField(relay, "batchSize", 10);
        ReflectionTestUtils.setField(relay, "sendTimeoutSeconds", 5L);
        ReflectionTestUtils.setField(relay, "maxAttempts", 3);
        new TransactionTemplate(transactionManager).executeWithoutResult(ignored -> relay.relayResults());
    }

    private KafkaConsumer<String, String> freshConsumer(String topic) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "match-outbox-proof-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private ConsumerRecord<String, String> awaitRecord(
            KafkaConsumer<String, String> consumer,
            String topic,
            String description,
            UUID expectedEventId) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                if (topic.equals(record.topic()) && hasEventId(record, expectedEventId)) {
                    return record;
                }
            }
        }
        fail("Timed out waiting for " + description);
        throw new AssertionError("unreachable");
    }

    private boolean hasEventId(ConsumerRecord<String, String> record, UUID expectedEventId) {
        try {
            return expectedEventId.toString().equals(
                    objectMapper.readTree(record.value()).path("eventId").asText());
        } catch (Exception ignored) {
            return false;
        }
    }

    private long endOffset(String topic) throws Exception {
        TopicPartition partition = new TopicPartition(topic, 0);
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            return admin.listOffsets(Map.of(partition, OffsetSpec.latest()))
                    .all().get().get(partition).offset();
        }
    }

    private void await(String description, BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting for " + description, exception);
            }
        }
        fail("Timed out waiting for " + description);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class IntegrationDependencies {
        @Bean
        @Primary
        SettlementEligibilityClient alwaysEligibleForKafkaBoundaryProof() {
            return (shipperId, codAmount) -> Mono.just(true);
        }
    }
}
