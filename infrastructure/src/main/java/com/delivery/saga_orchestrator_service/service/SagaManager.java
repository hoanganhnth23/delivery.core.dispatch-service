package com.delivery.saga_orchestrator_service.service;

import com.delivery.dispatch.application.DefaultDeliveryProgressUseCase;
import com.delivery.dispatch.application.DefaultAssignmentUseCase;
import com.delivery.dispatch.application.DefaultDeliveryCreationUseCase;
import com.delivery.dispatch.application.api.DeliveryCreationUseCase;
import com.delivery.dispatch.application.DefaultStepFailureUseCase;
import com.delivery.dispatch.application.api.StepFailureUseCase;
import com.delivery.dispatch.application.DefaultMatchOutcomeUseCase;
import com.delivery.dispatch.application.api.AssignmentUseCase;
import com.delivery.dispatch.application.DefaultOrderLifecycleUseCase;
import com.delivery.dispatch.application.api.DispatchCase;
import com.delivery.dispatch.application.api.MatchOutcomeUseCase;
import com.delivery.dispatch.application.api.OfferCommands;
import com.delivery.dispatch.application.api.OrderLifecycleUseCase;
import com.delivery.dispatch.application.api.DeliveryProgressUseCase;
import com.delivery.dispatch.domain.AssignmentPolicy;
import com.delivery.dispatch.domain.CaseHistory;
import com.delivery.dispatch.domain.DispatchStatus;
import com.delivery.dispatch.domain.FailureCompensation;
import com.delivery.dispatch.domain.MatchingCommandAssembly;
import com.delivery.dispatch.domain.MatchingCommandPolicy;
import com.delivery.dispatch.domain.MatchingRetrySettings;
import com.delivery.dispatch.domain.MatchingSession;
import com.delivery.dispatch.domain.OfferRetirementPolicy;
import com.delivery.dispatch.domain.RematchPolicy;
import com.delivery.dispatch.domain.ShipperOffer;
import com.delivery.saga_orchestrator_service.entity.SagaInstance;
import com.delivery.saga_orchestrator_service.entity.SagaInstance.SagaStatus;
import com.delivery.saga_orchestrator_service.entity.SagaEarlyEvent;
import com.delivery.saga_orchestrator_service.repository.SagaInstanceRepository;
import com.delivery.saga_orchestrator_service.repository.SagaInboundReceiptRepository;
import com.delivery.saga_orchestrator_service.repository.SagaEarlyEventRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * ✅ Active Saga Manager — "Nhạc trưởng" phát lệnh điều phối luồng đặt hàng
 *
 * Flow:
 * 1. order.created → SAGA → [saga.command.create-delivery] → Delivery
 * 2. delivery.created.result → SAGA → [saga.command.find-shipper] → Match
 * 3. shipper.found → SAGA → [saga.command.cache-shipper-found] → Delivery
 *                          → [saga.command.update-order-status] → Order
 * 4. shipper.not-found → SAGA → [saga.command.update-order-status](SHIPPER_NOT_FOUND) → Order
 *                              → [saga.command.mark-shipper-not-found] → Delivery
 * 5. delivery.shipper-accepted → SAGA → [saga.command.update-order-status] → Order
 * 6. delivery.status-updated → SAGA → [saga.command.update-order-status] → Order
 */
@Slf4j
@Service
public class SagaManager {

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaOutboxService outboxService;
    private final SagaInboundReceiptRepository inboundReceiptRepository;
    private final SagaEarlyEventRepository earlyEventRepository;
    private final ObjectMapper objectMapper;

    // ======== Saga Command Topics ========
    public static final String CMD_CREATE_DELIVERY = "saga.command.create-delivery";
    public static final String CMD_CANCEL_DELIVERY = "saga.command.cancel-delivery";
    public static final String CMD_FIND_SHIPPER = "saga.command.find-shipper";
    public static final String CMD_CACHE_SHIPPER_FOUND = "saga.command.cache-shipper-found";
    public static final String CMD_EXPIRE_SHIPPER_OFFER = "saga.command.expire-shipper-offer";
    public static final String CMD_MARK_SHIPPER_NOT_FOUND = "saga.command.mark-shipper-not-found";
    public static final String CMD_STOP_MATCHING = "saga.command.stop-matching";
    public static final String CMD_UPDATE_ORDER_STATUS = "saga.command.update-order-status";

    @Value("${app.kafka.topics.create-delivery:saga.command.create-delivery}")
    private String createDeliveryTopic = CMD_CREATE_DELIVERY;
    @Value("${app.kafka.topics.cancel-delivery:saga.command.cancel-delivery}")
    private String cancelDeliveryTopic = CMD_CANCEL_DELIVERY;
    @Value("${app.kafka.topics.find-shipper:saga.command.find-shipper}")
    private String findShipperTopic = CMD_FIND_SHIPPER;
    @Value("${app.kafka.topics.cache-shipper-found:saga.command.cache-shipper-found}")
    private String cacheShipperFoundTopic = CMD_CACHE_SHIPPER_FOUND;
    @Value("${app.kafka.topics.expire-shipper-offer:saga.command.expire-shipper-offer}")
    private String expireShipperOfferTopic = CMD_EXPIRE_SHIPPER_OFFER;
    @Value("${app.kafka.topics.mark-shipper-not-found:saga.command.mark-shipper-not-found}")
    private String markShipperNotFoundTopic = CMD_MARK_SHIPPER_NOT_FOUND;
    @Value("${app.kafka.topics.stop-matching:saga.command.stop-matching}")
    private String stopMatchingTopic = CMD_STOP_MATCHING;
    @Value("${app.kafka.topics.update-order-status:saga.command.update-order-status}")
    private String updateOrderStatusTopic = CMD_UPDATE_ORDER_STATUS;
    @Value("${matching.initial.max-retry-attempts:10}")
    private int initialMatchMaxRetryAttempts = 10;
    @Value("${matching.initial.delay-seconds:30}")
    private int initialMatchDelaySeconds = 30;
    @Value("${matching.initial.max-delay-seconds:300}")
    private int initialMatchMaxDelaySeconds = 300;
    @Value("${matching.initial.backoff-multiplier:1.5}")
    private double initialMatchBackoffMultiplier = 1.5;
    @Value("${app.saga.timeout.finding-shipper-minutes:5}")
    private int findingShipperTimeoutMinutes = 5;
    @Value("${matching.batch.client-capability-enabled:false}")
    private boolean batchClientCapabilityEnabled;
    @Value("${spring.datasource.url:}")
    private String dataSourceUrl;

    @Autowired
    public SagaManager(SagaInstanceRepository sagaInstanceRepository,
                       SagaOutboxService outboxService,
                       SagaInboundReceiptRepository inboundReceiptRepository,
                       SagaEarlyEventRepository earlyEventRepository) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.outboxService = outboxService;
        this.inboundReceiptRepository = inboundReceiptRepository;
        this.earlyEventRepository = earlyEventRepository;
        this.objectMapper = new ObjectMapper();
    }

    /** Compatibility constructor for tests that exercise the durable inbox but not early-event staging. */
    public SagaManager(SagaInstanceRepository sagaInstanceRepository,
                       SagaOutboxService outboxService,
                       SagaInboundReceiptRepository inboundReceiptRepository) {
        this(sagaInstanceRepository, outboxService, inboundReceiptRepository, null);
    }

    /** Compatibility constructor for focused tests that do not exercise the inbox boundary. */
    public SagaManager(SagaInstanceRepository sagaInstanceRepository, SagaOutboxService outboxService) {
        this(sagaInstanceRepository, outboxService, null, null);
    }

    // ==================== EVENT HANDLERS ====================

    /**
     * Step 1: order.created → Tạo saga + phát lệnh tạo delivery
     */
    @Transactional
    public void handleOrderCreated(Long orderId, String rawEvent) {
        if (!claimInbound("order.created", orderId, rawEvent)) return;
        // Idempotent check
        SagaInstance existing = sagaInstanceRepository.findByOrderIdForUpdate(orderId).orElse(null);
        if (existing != null) {
            if (sameJson(existing.getPayload(), rawEvent)) {
                log.info("[Saga] Exact order.created replay for orderId={}, skipping", orderId);
                return;
            }
            throw new IllegalStateException("order.created conflicts with existing Saga payload for orderId="
                    + orderId);
        }

        SagaInstance saga = new SagaInstance();
        saga.setSagaType("ORDER_CREATION");
        saga.setOrderId(orderId);
        saga.setStatus(SagaStatus.STARTED);
        saga.setPayload(rawEvent);
        saga.addStep("ORDER_CREATED", "order.created", rawEvent);
        sagaInstanceRepository.save(saga);

        log.info("🆕 [Saga] Created saga for orderId={}, id={}", orderId, saga.getId());

        // A cancellation/restaurant decision may have arrived on another topic
        // while order.created was delayed. Promote those durable facts before
        // dispatching create-delivery so cancellation cannot create an orphan.
        drainEarlyEventsForSaga(saga);
        if (saga.getStatus() != SagaStatus.STARTED) {
            log.info("[Saga] orderId={} has early terminal/advance fact; skipping create-delivery in status={}",
                    orderId, saga.getStatus());
            return;
        }

        // ✅ PHÁT LỆNH: Tạo delivery
        sendCommand(CMD_CREATE_DELIVERY, orderId.toString(), rawEvent);
        log.info("📤 [Saga] Sent command: {} for orderId={}", CMD_CREATE_DELIVERY, orderId);
    }

    /**
     * Step 2: delivery.created.result → Delivery đã tạo.
     * ✅ GATE: KHÔNG tìm shipper ngay. Chỉ tìm shipper SAU KHI nhà hàng confirm đơn
     * (restaurant.order-confirmed). Nếu nhà hàng đã confirm trước đó (race) thì tìm luôn.
     */
    @Transactional
    public void handleDeliveryCreated(Long orderId, Long deliveryId, String rawEvent) {
        if (!claimInbound("delivery.created.result", orderId, rawEvent)) return;
        SagaInstance saga = findSagaByOrderId(orderId);
        switch (deliveryCreation().onDeliveryCreated(new SagaDispatchCase(saga, objectMapper), deliveryId, rawEvent)) {
            case REPLAY -> log.info("[Saga] Exact delivery-created replay for orderId={}, deliveryId={}, skipping",
                    orderId, deliveryId);
            case ORPHAN_CANCELLED -> log.info("[Saga] Late delivery-created result for terminal orderId={}, "
                    + "deliveryId={}; cancellation re-issued after {}", orderId, deliveryId, saga.getStatus());
            case AWAITING_RESTAURANT ->
                    log.info("⏸️ [Saga] Delivery tạo xong, CHỜ nhà hàng confirm mới tìm shipper. orderId={}", orderId);
            case MATCHING_STARTED ->
                    log.info("🍽️ [Saga] Nhà hàng đã confirm trước; tìm shipper ngay cho orderId={}", orderId);
        }
    }

    /**
     * ✅ restaurant.order-confirmed → Mở cổng tìm shipper.
     * Xử lý cả 2 thứ tự: confirm đến trước hay sau delivery.created.result.
     */
    @Transactional
    public void handleRestaurantConfirmed(Long orderId, String rawEvent) {
        SagaInstance saga = sagaInstanceRepository.findByOrderIdForUpdate(orderId).orElse(null);
        if (saga == null) {
            stageEarlyEvent("restaurant.order-confirmed", orderId, rawEvent);
            return;
        }
        if (!claimInbound("restaurant.order-confirmed", orderId, rawEvent)) return;
        applyRestaurantConfirmedLocked(saga, rawEvent);
    }

    private void applyRestaurantConfirmedLocked(SagaInstance saga, String rawEvent) {
        Long orderId = saga.getOrderId();
        switch (orderLifecycle().confirmRestaurant(new SagaDispatchCase(saga, objectMapper), rawEvent)) {
            case ALREADY_CONFIRMED ->
                    log.warn("⚠️ [Saga] Nhà hàng đã confirm trước đó cho orderId={}, bỏ qua (idempotent)", orderId);
            case IGNORED_STATE ->
                    log.warn("⚠️ [Saga] handleRestaurantConfirmed - orderId={} đang ở {}, bỏ qua", orderId, saga.getStatus());
            case AWAITING_DELIVERY -> log.info(
                    "🍽️ [Saga] Nhà hàng confirm orderId={} nhưng delivery chưa sẵn sàng, sẽ tìm shipper sau", orderId);
            case MATCHING_STARTED -> log.info("🍽️ [Saga] Nhà hàng confirm orderId={} → tìm shipper", orderId);
        }
    }

    /**
     * Phát lệnh tìm shipper (kèm cấu hình retry) + chuyển saga sang FINDING_SHIPPER.
     * Dùng chung cho: delivery-created (đã confirm) và restaurant-confirmed (delivery đã tạo).
     */
    private void triggerFindShipper(SagaInstance saga, Long orderId, Long deliveryId, String deliveryResultEvent) {
        String modifiedEvent;
        try {
            ObjectNode payloadNode = buildFindShipperPayload(saga, deliveryResultEvent);
            putRetrySettings(payloadNode, new MatchingRetrySettings(initialMatchMaxRetryAttempts,
                    initialMatchDelaySeconds, initialMatchMaxDelaySeconds, initialMatchBackoffMultiplier));
            payloadNode.put("matchingDeadlineAt", MatchingCommandPolicy
                    .initialDeadline(LocalDateTime.now(), findingShipperTimeoutMinutes).toString());

            modifiedEvent = dispatchFindShipperCommand(saga, orderId, payloadNode);
            sendOrderStatusCommand(orderId, "FINDING_SHIPPER", modifiedEvent);
            log.info("📤 [Saga] Sent command: {} for orderId={}, deliveryId={} with retry settings", CMD_FIND_SHIPPER, orderId, deliveryId);
        } catch (Exception e) {
            if (e instanceof SagaCommandPublishException publishException) {
                throw publishException;
            }
            throw new IllegalStateException("Cannot build canonical find-shipper command", e);
        }

        saga.setStatus(SagaStatus.FINDING_SHIPPER);
        sagaInstanceRepository.save(saga);
    }

    /**
     * Step 3a: shipper.found → request Delivery persist the offer. Order must
     * remain FINDING_SHIPPER until Delivery confirms its own transaction.
     */
    @Transactional
    public void handleShipperFound(Long orderId, Long deliveryId, String rawEvent) {
        if (!claimInbound("shipper.found", orderId, rawEvent)) return;
        SagaInstance saga = findSagaByOrderId(orderId);
        requireDeliveryIdentity(saga, deliveryId, orderId);
        switch (matchOutcomes().onShipperFound(new SagaDispatchCase(saga, objectMapper),
                () -> isCurrentMatchingResult(saga, rawEvent), rawEvent)) {
            case STALE -> log.info("[Saga] Ignoring stale shipper.found generation for orderId={}", orderId);
            case IGNORED_STATE -> log.warn("⚠️ [Saga] handleShipperFound - Saga cho orderId={} đang ở {}, bỏ qua event",
                    orderId, saga.getStatus());
            default -> log.info("📤 [Saga] Sent cache-shipper command for orderId={}; awaiting Delivery confirmation",
                    orderId);
        }
    }

    /** Delivery owns the offer. Only this committed confirmation may expose WAIT to Order. */
    @Transactional
    public void handleOfferPersisted(Long orderId, Long deliveryId, String rawEvent) {
        if (!claimInbound("delivery.offer-persisted", orderId, rawEvent)) return;
        SagaInstance saga = findSagaByOrderId(orderId);
        requireDeliveryIdentity(saga, deliveryId, orderId);
        UUID sourceCommandId;
        try {
            JsonNode event = objectMapper.readTree(rawEvent);
            sourceCommandId = UUID.fromString(event.path("sourceCommandEventId").asText());
            UUID.fromString(event.path("matchingSessionId").asText());
            if (event.path("offeredShipperId").asLong() <= 0 || !event.hasNonNull("offerExpiresAt")) {
                throw new IllegalArgumentException("offer-persisted identity is incomplete");
            }
        } catch (IllegalArgumentException invalid) {
            throw invalid;
        } catch (Exception invalid) {
            throw new IllegalArgumentException("Invalid delivery.offer-persisted payload", invalid);
        }
        switch (matchOutcomes().onOfferPersisted(new SagaDispatchCase(saga, objectMapper), sourceCommandId,
                () -> isCurrentMatchingResult(saga, rawEvent), rawEvent)) {
            case STRONGER_STATE -> log.info("[Saga] Offer confirmation arrived after stronger state {} for orderId={}",
                    saga.getStatus(), orderId);
            case STALE -> log.info("[Saga] Ignoring stale/unexpected offer confirmation for orderId={}", orderId);
            default -> { }
        }
    }

    /**
     * Step 3b: shipper.not-found → Compensation: cancel delivery + update order FAILED
     */
    @Transactional
    public void handleShipperNotFound(Long orderId, Long deliveryId, String rawEvent) {
        if (!claimInbound("shipper.not-found", orderId, rawEvent)) return;
        SagaInstance saga = findSagaByOrderId(orderId);
        requireDeliveryIdentity(saga, deliveryId, orderId);
        switch (matchOutcomes().onShipperNotFound(new SagaDispatchCase(saga, objectMapper),
                () -> isCurrentMatchingResult(saga, rawEvent), rawEvent)) {
            case STALE -> log.info("[Saga] Ignoring stale shipper.not-found generation for orderId={}", orderId);
            case IGNORED_STATE -> log.warn("⚠️ [Saga] handleShipperNotFound - Saga cho orderId={} đang ở {}, bỏ qua event",
                    orderId, saga.getStatus());
            default -> log.warn("🚨 [Saga] COMPENSATION — shipper not found, orderId={}", orderId);
        }
    }

    /**
     * Step 4: delivery.shipper-accepted → Cập nhật order status
     */
    @Transactional
    public void handleShipperAccepted(Long orderId, Long deliveryId, Long shipperId, String rawEvent) {
        if (!claimInbound("delivery.shipper-accepted", orderId, rawEvent)) return;
        SagaInstance saga = findSagaByOrderId(orderId);
        requireDeliveryIdentity(saga, deliveryId, orderId);
        // Acceptance and rejection travel on different topics; a delayed acceptance from an
        // excluded shipper must not resurrect it, while one overtaking an offer timeout may assign.
        switch (assignments().onAccepted(new SagaDispatchCase(saga, objectMapper), shipperId, rawEvent)) {
            case REPLAY -> log.info("[Saga] Exact shipper-accepted replay for orderId={}, shipperId={}, skipping",
                    orderId, shipperId);
            case IGNORED_REJECTED_SHIPPER -> log.info(
                    "[Saga] Ignoring stale acceptance from rejected shipper {} for orderId={}", shipperId, orderId);
            case IGNORED_STATE -> log.warn("⚠️ [Saga] handleShipperAccepted - Saga cho orderId={} đang ở {}, bỏ qua event",
                    orderId, saga.getStatus());
            case ASSIGNED -> log.info("📤 [Saga] Shipper {} assigned, sent update-order for orderId={}",
                    shipperId, orderId);
        }
    }

    /**
     * Step 4b: delivery.shipper-rejected → Re-trigger tìm shipper mới (loại trừ shipper đã reject)
     */
    @Transactional
    public void handleShipperRejected(Long orderId, Long deliveryId, Long rejectedShipperId, String rawEvent) {
        if (!claimInbound("delivery.shipper-rejected", orderId, rawEvent)) return;
        SagaInstance saga = findSagaByOrderId(orderId);
        requireDeliveryIdentity(saga, deliveryId, orderId);
        // Pre-accept rejection and post-accept cancel-assignment both rematch.
        switch (assignments().onRejected(new SagaDispatchCase(saga, objectMapper), rejectedShipperId, rawEvent)) {
            case IGNORED_STATE -> log.warn("⚠️ [Saga] handleShipperRejected - Saga cho orderId={} đang ở {}, bỏ qua event",
                    orderId, saga.getStatus());
            case DUPLICATE -> log.info("[Saga] Duplicate rejection from shipper {} for orderId={}, skipping",
                    rejectedShipperId, orderId);
            case EXHAUSTED -> log.warn("🚨 [Saga] Too many shipper rejections for orderId={}, failing saga", orderId);
            case REMATCHED -> log.info("🔄 [Saga] Shipper {} rejected orderId={}, re-triggered find-shipper",
                    rejectedShipperId, orderId);
        }
    }

    /**
     * Applies a scheduler observation only when the Saga has not changed since
     * it was selected. This is deliberately a first-class inbox command rather
     * than anonymous synthetic JSON: a repeated poll is an exact replay and a
     * stale poll cannot compensate a newer state.
     */
    @Transactional
    public void handleTimeout(SagaTimeoutCommand command) {
        if (command == null || command.orderId() == null || command.orderId() <= 0
                || command.eventId() == null || command.expectedStatus() == null) {
            throw new IllegalArgumentException("Timeout command identity, orderId and expected status are required");
        }
        SagaInstance saga = findSagaByOrderId(command.orderId());
        if (!matchesTimeoutObservation(saga, command)) {
            log.info("[Saga] Ignoring stale timeout for orderId={} expected={}/{} observed={}/{}",
                    command.orderId(), command.expectedStatus(), command.expectedVersion(),
                    saga.getStatus(), versionOf(saga));
            return;
        }

        // A shipper-offer candidate is intentionally queried from a very small
        // minimum age. Do not consume an inbox receipt until its exact offer
        // deadline has actually elapsed.
        if (command.expectedStatus() == SagaStatus.SHIPPER_FOUND) {
            if (!isShipperOfferTimeoutDue(saga)) {
                return;
            }
        } else if (LocalDateTime.now().isBefore(command.deadline())) {
            log.info("[Saga] Ignoring early timeout for orderId={} status={} deadline={}",
                    command.orderId(), command.expectedStatus(), command.deadlineAt());
            return;
        }

        String rawTimeoutEvent = command.toJson(objectMapper);
        if (!claimInbound("saga.timeout." + command.expectedStatus().name(),
                command.orderId(), rawTimeoutEvent)) {
            return;
        }

        if (command.expectedStatus() == SagaStatus.SHIPPER_FOUND) {
            handleShipperOfferTimeoutLocked(saga, rawTimeoutEvent);
            return;
        }
        handleStepFailedLocked("TIMEOUT_" + command.expectedStatus().name(), saga,
                command.reason(), rawTimeoutEvent);
    }

    /**
     * Compatibility entry point for focused callers. Production scheduling uses
     * {@link #handleTimeout(SagaTimeoutCommand)} so it carries a snapshot fence.
     */
    @Transactional
    public void handleShipperOfferTimeout(Long orderId) {
        SagaInstance saga = findSagaByOrderId(orderId);
        if (saga.getStatus() != SagaStatus.SHIPPER_FOUND) {
            return;
        }
        handleTimeout(SagaTimeoutCommand.forShipperOffer(saga, "Shipper offer timeout"));
    }

    /**
     * A shipper did not answer the single active offer. Re-run matching with the
     * timed-out shipper excluded; only compensate after the shared attempt limit.
     * The aggregate is already locked and the timeout has already been claimed.
     */
    private void handleShipperOfferTimeoutLocked(SagaInstance saga, String rawTimeoutEvent) {
        Long orderId = saga.getOrderId();

        CaseHistory history = history(saga);
        CaseHistory.Fact foundStep = history.latestFact("SHIPPER_FOUND");
        if (foundStep == null || foundStep.eventData() == null) {
            handleStepFailedLocked("SHIPPER_OFFER_TIMEOUT", saga,
                    "Missing shipper offer payload", rawTimeoutEvent);
            return;
        }

        long previousFailedOffers = history.countWithPrefix("SHIPPER_REJECTED")
                + history.countWithPrefix("SHIPPER_OFFER_TIMEOUT");
        // The shared limit is checked before parsing so an exhausted case with a
        // malformed offer still compensates as exhausted.
        if (previousFailedOffers >= RematchPolicy.MAX_FAILED_OFFERS) {
            handleStepFailedLocked("SHIPPER_OFFER_TIMEOUT_LIMIT", saga,
                    "Shipper offer attempts exhausted", rawTimeoutEvent);
            return;
        }

        try {
            ObjectNode payload = buildFindShipperPayload(saga, foundStep.eventData());
            JsonNode foundPayload = objectMapper.readTree(foundStep.eventData());
            if (!foundPayload.has("availableShippers")
                    || !foundPayload.get("availableShippers").isArray()
                    || foundPayload.get("availableShippers").size() != 1
                    || !foundPayload.get("availableShippers").get(0).hasNonNull("shipperId")) {
                throw new IllegalStateException("Offer payload must contain exactly one shipper");
            }
            long timedOutShipperId = foundPayload.get("availableShippers").get(0)
                    .get("shipperId").asLong();
            if (timedOutShipperId <= 0) {
                throw new IllegalStateException("Offer payload shipperId must be positive");
            }
            ShipperOffer offer = new ShipperOffer(timedOutShipperId,
                    foundPayload.hasNonNull("foundAt")
                            ? LocalDateTime.parse(foundPayload.get("foundAt").asText())
                            : foundStep.executedAt(),
                    foundPayload.hasNonNull("waitingTimeoutSeconds")
                            ? foundPayload.get("waitingTimeoutSeconds").asInt()
                            : null);
            LocalDateTime offerExpiresAt = offer.expiresAt();
            if (offerExpiresAt.isAfter(LocalDateTime.now())) {
                log.debug("[Saga] Offer is still active for orderId={} until {}, skipping timeout poll",
                        orderId, offerExpiresAt);
                return;
            }

            RematchPolicy.Rematch rematch = (RematchPolicy.Rematch) RematchPolicy.onOfferTimeout(
                    timedOutShipperId, history.recordedRejectedShippers(), previousFailedOffers);
            payload.put("rejectedShipperId", timedOutShipperId);

            var excludedArray = objectMapper.createArrayNode();
            rematch.excludedShipperIds().forEach(excludedArray::add);
            payload.set("excludedShipperIds", excludedArray);
            putRetrySettings(payload, MatchingRetrySettings.REMATCH);

            // The prepared rematch is persisted now but dispatched only after
            // Delivery acknowledges retirement (delivery.offer-retired), so an
            // acceptance committed near the deadline cannot race a new offer.
            String rematchEvent = objectMapper.writeValueAsString(payload);
            saga.setStatus(SagaStatus.OFFER_RETIRING);
            saga.addStep("SHIPPER_OFFER_TIMEOUT_" + rematch.attempt(),
                    "shipper.offer-timeout", rematchEvent);

            ObjectNode expireCommand = objectMapper.createObjectNode();
            expireCommand.put("orderId", orderId);
            expireCommand.put("deliveryId", saga.getDeliveryId());
            expireCommand.put("timedOutShipperId", timedOutShipperId);
            expireCommand.put("expectedOfferExpiresAt", offerExpiresAt.toString());
            UUID matchingSessionId = currentMatchingSessionId(saga);
            if (matchingSessionId != null) {
                expireCommand.put("matchingSessionId", matchingSessionId.toString());
            }

            UUID expireCommandId = sendCommand(CMD_EXPIRE_SHIPPER_OFFER, orderId.toString(),
                    objectMapper.writeValueAsString(expireCommand));
            ObjectNode requested = objectMapper.createObjectNode();
            requested.put("expireCommandEventId", expireCommandId == null ? null : expireCommandId.toString());
            saga.addStep("OFFER_RETIRE_REQUESTED", CMD_EXPIRE_SHIPPER_OFFER, requested.toString());
            sagaInstanceRepository.save(saga);
            log.info("🔄 [Saga] Offer timed out for shipper {}, awaiting retirement before rematching "
                    + "orderId={} exclusions={}", timedOutShipperId, orderId, rematch.excludedShipperIds());
        } catch (Exception e) {
            if (e instanceof SagaCommandPublishException publishException) {
                throw publishException;
            }
            log.error("[Saga] Cannot build offer-timeout rematch command for orderId={}", orderId, e);
            handleStepFailedLocked("SHIPPER_OFFER_TIMEOUT", saga,
                    "Cannot build rematch command: " + e.getMessage(), rawTimeoutEvent);
        }
    }

    /**
     * delivery.offer-retired → Delivery acknowledged the expire command. Only
     * the acknowledgement of the currently requested expiry may advance the case.
     */
    @Transactional
    public void handleOfferRetired(Long orderId, Long deliveryId, String rawEvent) {
        if (!claimInbound("delivery.offer-retired", orderId, rawEvent)) return;
        SagaInstance saga = findSagaByOrderId(orderId);
        requireDeliveryIdentity(saga, deliveryId, orderId);
        JsonNode event;
        UUID sourceCommandId;
        try {
            event = objectMapper.readTree(rawEvent);
            sourceCommandId = UUID.fromString(event.path("sourceCommandEventId").asText());
        } catch (Exception invalid) {
            throw new IllegalArgumentException("Invalid delivery.offer-retired payload", invalid);
        }
        Long shipperId = event.hasNonNull("shipperId") ? event.get("shipperId").asLong() : null;
        OfferRetirementPolicy.Decision decision = OfferRetirementPolicy.decide(
                event.hasNonNull("outcome") ? event.get("outcome").asText() : null, shipperId);
        switch (matchOutcomes().onOfferRetired(new SagaDispatchCase(saga, objectMapper), sourceCommandId,
                decision, shipperId, rawEvent)) {
            case STALE -> log.info("[Saga] Ignoring stale/unexpected offer retirement for orderId={} status={}",
                    orderId, saga.getStatus());
            case REMATCHED -> log.info("🔄 [Saga] Offer retired, rematching orderId={}", orderId);
            case ASSIGNED -> log.info("[Saga] Offer retirement reported committed assignment of shipper {} for orderId={}",
                    shipperId, orderId);
            default -> log.info("[Saga] Offer retirement found terminal Delivery for orderId={}; awaiting its terminal fact",
                    orderId);
        }
    }

    /**
     * Step 5: delivery.status-updated → Forward status đến order-service
     */
    @Transactional
    public void handleDeliveryStatusUpdated(Long orderId, Long deliveryId, String newStatus, String rawEvent) {
        if (!claimInbound("delivery.status-updated", orderId, rawEvent)) return;
        SagaInstance saga = findSagaByOrderId(orderId);
        requireDeliveryIdentity(saga, deliveryId, orderId);

        SagaDispatchCase dispatchCase = new SagaDispatchCase(saga, objectMapper);
        switch (deliveryProgress().apply(dispatchCase, newStatus, rawEvent)) {
            case REPLAY -> log.info("[Saga] Exact delivery status replay {} for orderId={}, skipping",
                    newStatus, orderId);
            case SHIPPER_NOT_FOUND_ECHO_RECORDED ->
                    log.info("📥 [Saga] Recorded delivery SHIPPER_NOT_FOUND terminal echo for orderId={}", orderId);
            case CANCELLATION_CONFIRMED -> log.info("[Saga] Recorded delivery cancellation confirmation for "
                    + "terminal/compensating orderId={} status={}", orderId, saga.getStatus());
            case APPLIED -> log.info("📤 [Saga] Delivery status={}, forwarded to order for orderId={}",
                    newStatus, orderId);
        }
    }

    private DeliveryProgressUseCase deliveryProgress() {
        return new DefaultDeliveryProgressUseCase(
                dispatchCase -> sagaInstanceRepository.save(((SagaDispatchCase) dispatchCase).saga()),
                (dispatchCase, status, cause) -> sendOrderStatusCommand(
                        ((SagaDispatchCase) dispatchCase).saga(), status, cause),
                this::sameJson);
    }

    /**
     * order.cancelled → Compensation: cancel delivery + stop matching
     */
    @Transactional
    public void handleOrderCancelled(Long orderId, String rawEvent) {
        SagaInstance saga = sagaInstanceRepository.findByOrderIdForUpdate(orderId).orElse(null);
        if (saga == null) {
            stageEarlyEvent("order.cancelled", orderId, rawEvent);
            return;
        }
        if (!claimInbound("order.cancelled", orderId, rawEvent)) return;
        applyOrderCancelledLocked(saga, rawEvent);
    }

    private void applyOrderCancelledLocked(SagaInstance saga, String rawEvent) {
        Long orderId = saga.getOrderId();
        switch (orderLifecycle().cancelOrder(new SagaDispatchCase(saga, objectMapper), rawEvent)) {
            case REPLAY -> log.info("[Saga] Exact order-cancelled replay for orderId={}, skipping", orderId);
            case IGNORED_FAILED -> log.info("Ignoring cancellation consequence for failed Saga orderId={}", orderId);
            case CANCELLED, COMPENSATING ->
                    log.warn("🚨 [Saga] COMPENSATION — order cancelled, orderId={}", orderId);
        }
    }

    /** Cancels the Delivery and stops the current matching generation, enriched with deliveryId. */
    private void cancelDeliveryAndStopMatching(SagaInstance saga, String rawEvent) {
        Long orderId = saga.getOrderId();
        try {
            ObjectNode payloadNode = (ObjectNode) objectMapper.readTree(rawEvent);
            if (saga.getDeliveryId() != null) {
                payloadNode.put("deliveryId", saga.getDeliveryId());
            }
            String enrichedEvent = objectMapper.writeValueAsString(payloadNode);
            sendCommand(CMD_CANCEL_DELIVERY, orderId.toString(), enrichedEvent);
            sendStopMatchingCommand(saga, orderId, enrichedEvent);
        } catch (Exception e) {
            if (e instanceof SagaCommandPublishException publishException) {
                throw publishException;
            }
            sendCommand(CMD_CANCEL_DELIVERY, orderId.toString(), rawEvent);
            sendStopMatchingCommand(saga, orderId, rawEvent);
        }
    }

    private DeliveryCreationUseCase deliveryCreation() {
        return new DefaultDeliveryCreationUseCase(
                dispatchCase -> sagaInstanceRepository.save(saga(dispatchCase)),
                (dispatchCase, deliveryEvent) -> {
                    SagaInstance saga = saga(dispatchCase);
                    triggerFindShipper(saga, saga.getOrderId(), saga.getDeliveryId(), deliveryEvent);
                },
                (dispatchCase, cause) -> sendCommand(CMD_CANCEL_DELIVERY, String.valueOf(dispatchCase.orderId()), cause),
                (dispatchCase, status, cause) -> sendOrderStatusCommand(saga(dispatchCase), status, cause));
    }

    private StepFailureUseCase stepFailures() {
        return new DefaultStepFailureUseCase(
                dispatchCase -> sagaInstanceRepository.save(saga(dispatchCase)),
                (dispatchCase, deliveryCommand, stopMatching, cause) ->
                        compensateDelivery(saga(dispatchCase), deliveryCommand, stopMatching, cause),
                (dispatchCase, status, cause) -> sendOrderStatusCommand(saga(dispatchCase), status, cause));
    }

    /** Sends the compensating Delivery command enriched with deliveryId; returns the correlated event. */
    private String compensateDelivery(SagaInstance saga, FailureCompensation.DeliveryCommand deliveryCommand,
                                      boolean stopMatching, String rawEvent) {
        Long orderId = saga.getOrderId();
        String topic = deliveryCommand == FailureCompensation.DeliveryCommand.CANCEL_DELIVERY
                ? CMD_CANCEL_DELIVERY
                : CMD_MARK_SHIPPER_NOT_FOUND;
        String correlatedEvent = rawEvent;
        try {
            ObjectNode payloadNode = (ObjectNode) objectMapper.readTree(rawEvent);
            if (saga.getDeliveryId() != null) {
                payloadNode.put("deliveryId", saga.getDeliveryId());
            }
            String enrichedEvent = objectMapper.writeValueAsString(payloadNode);
            correlatedEvent = enrichedEvent;
            sendCommand(topic, orderId.toString(), enrichedEvent);
            if (stopMatching) {
                sendStopMatchingCommand(saga, orderId, enrichedEvent);
            }
        } catch (Exception e) {
            if (e instanceof SagaCommandPublishException publishException) {
                throw publishException;
            }
            sendCommand(topic, orderId.toString(), rawEvent);
        }
        return correlatedEvent;
    }

    private AssignmentUseCase assignments() {
        return new DefaultAssignmentUseCase(
                dispatchCase -> sagaInstanceRepository.save(saga(dispatchCase)),
                (dispatchCase, cause, excluded) -> rematchAfterRejection(saga(dispatchCase), cause, excluded),
                offerCommands(),
                (dispatchCase, status, cause) -> sendOrderStatusCommand(saga(dispatchCase), status, cause));
    }

    /** Canonical rematch with the fixed rematch budget and ordered exclusions. */
    private void rematchAfterRejection(SagaInstance saga, String causeEvent, List<Long> excludedShipperIds) {
        Long orderId = saga.getOrderId();
        try {
            ObjectNode payloadNode = buildFindShipperPayload(saga, causeEvent);
            putRetrySettings(payloadNode, MatchingRetrySettings.REMATCH);
            var excludedArray = objectMapper.createArrayNode();
            excludedShipperIds.forEach(excludedArray::add);
            payloadNode.set("excludedShipperIds", excludedArray);
            dispatchFindShipperCommand(saga, orderId, payloadNode);
        } catch (Exception e) {
            if (e instanceof SagaCommandPublishException publishException) {
                throw publishException;
            }
            throw new IllegalStateException("Cannot build canonical rematch command for orderId=" + orderId, e);
        }
    }

    private MatchOutcomeUseCase matchOutcomes() {
        return new DefaultMatchOutcomeUseCase(
                dispatchCase -> sagaInstanceRepository.save(saga(dispatchCase)),
                offerCommands(),
                (dispatchCase, status, cause) -> sendOrderStatusCommand(saga(dispatchCase), status, cause));
    }

    private OfferCommands offerCommands() {
        return new OfferCommands() {
            @Override
            public void requestOfferPersistence(DispatchCase dispatchCase, String shipperFoundEvent) {
                SagaInstance saga = saga(dispatchCase);
                UUID cacheCommandId = sendCommand(CMD_CACHE_SHIPPER_FOUND, saga.getOrderId().toString(),
                        shipperFoundEvent);
                ObjectNode requested = objectMapper.createObjectNode();
                requested.put("cacheCommandEventId", cacheCommandId.toString());
                try {
                    requested.put("matchingSessionId",
                            objectMapper.readTree(shipperFoundEvent).path("matchingSessionId").asText());
                } catch (Exception invalid) {
                    throw new IllegalArgumentException("Invalid shipper.found payload", invalid);
                }
                saga.addStep("OFFER_PERSIST_REQUESTED", CMD_CACHE_SHIPPER_FOUND, requested.toString());
            }

            @Override
            public void markShipperNotFound(DispatchCase dispatchCase, String causeEvent) {
                sendCommand(CMD_MARK_SHIPPER_NOT_FOUND, String.valueOf(dispatchCase.orderId()), causeEvent);
            }

            @Override
            public String startPreparedRematch(DispatchCase dispatchCase, String preparedCommand) {
                SagaInstance saga = saga(dispatchCase);
                try {
                    return dispatchFindShipperCommand(saga, saga.getOrderId(),
                            (ObjectNode) objectMapper.readTree(preparedCommand));
                } catch (SagaCommandPublishException publishException) {
                    throw publishException;
                } catch (Exception malformed) {
                    throw new IllegalStateException("Prepared rematch is malformed for orderId="
                            + saga.getOrderId(), malformed);
                }
            }
        };
    }

    private OrderLifecycleUseCase orderLifecycle() {
        return new DefaultOrderLifecycleUseCase(
                dispatchCase -> sagaInstanceRepository.save(saga(dispatchCase)),
                (dispatchCase, deliveryEvent) -> {
                    SagaInstance saga = saga(dispatchCase);
                    triggerFindShipper(saga, saga.getOrderId(), saga.getDeliveryId(), deliveryEvent);
                },
                (dispatchCase, cause) -> cancelDeliveryAndStopMatching(saga(dispatchCase), cause),
                this::sameJson);
    }

    private static SagaInstance saga(DispatchCase dispatchCase) {
        return ((SagaDispatchCase) dispatchCase).saga();
    }

    // ==================== FAILURE HANDLERS ====================

    /**
     * ❌ delivery.created.failed → Tạo delivery thất bại → báo Order cancel
     */
    @Transactional
    public void handleDeliveryCreationFailed(Long orderId, String reason, String rawEvent) {
        if (!claimInbound("delivery.created.failed", orderId, rawEvent)) return;
        SagaInstance saga = findSagaByOrderId(orderId);
        switch (deliveryCreation().onDeliveryCreationFailed(new SagaDispatchCase(saga, objectMapper), rawEvent)) {
            case IGNORED_STATE -> log.warn("⚠️ [Saga] handleDeliveryCreationFailed - Saga cho orderId={} đang ở {}, "
                    + "bỏ qua (không phải STARTED)", orderId, saga.getStatus());
            case COMPENSATED -> log.error("🚨 [Saga] COMPENSATION — Delivery creation failed for orderId={}: {}",
                    orderId, reason);
        }
    }

    /**
     * ❌ Xử lý generic failure từ bất kỳ step nào
     */
    @Transactional
    public void handleStepFailed(String stepName, Long orderId, String reason, String rawEvent) {
        if (!claimInbound(stepName + ".failed", orderId, rawEvent)) return;
        SagaInstance saga = findSagaByOrderId(orderId);
        handleStepFailedLocked(stepName, saga, reason, rawEvent);
    }

    /**
     * Applies failure compensation while the Saga aggregate is already locked.
     * Scheduler-originated failures call this directly after their timeout inbox
     * command has been claimed, preventing a second claim with a different topic.
     */
    private void handleStepFailedLocked(String stepName, SagaInstance saga, String reason, String rawEvent) {
        Long orderId = saga.getOrderId();
        SagaStatus previous = saga.getStatus();
        switch (stepFailures().onStepFailed(new SagaDispatchCase(saga, objectMapper), stepName, rawEvent)) {
            case CANCEL_REFUSAL_RECORDED -> log.error("[Saga] Delivery cancellation failed after compensation for "
                    + "orderId={}: {}. Manual reconciliation is required.", orderId, reason);
            case IGNORED_TERMINAL -> log.warn("⚠️ [Saga] handleStepFailed - Saga cho orderId={} đã ở trạng thái cuối {}, bỏ qua",
                    orderId, saga.getStatus());
            case COMPENSATED -> log.error("🚨 [Saga] COMPENSATION — Step {} failed for orderId={} (prevStatus={}): {}",
                    stepName, orderId, previous, reason);
        }
    }

    /**
     * Replays one valid fact that was durably staged before its Saga existed.
     * The Saga row is locked before the early-event row, matching the creation
     * path's lock order and preventing a lock-order inversion.
     */
    @Transactional
    public void processEarlyEvent(UUID eventId) {
        if (earlyEventRepository == null || eventId == null) {
            return;
        }
        SagaEarlyEvent observed = earlyEventRepository.findById(eventId).orElse(null);
        if (observed == null) {
            return;
        }
        SagaInstance saga = sagaInstanceRepository.findByOrderIdForUpdate(observed.getOrderId()).orElse(null);
        if (saga == null) {
            return;
        }
        SagaEarlyEvent staged = earlyEventRepository.findByIdForUpdate(eventId).orElse(null);
        if (staged == null) {
            return;
        }
        applyEarlyEventLocked(saga, staged);
    }

    // ==================== HELPERS ====================

    private void drainEarlyEventsForSaga(SagaInstance saga) {
        if (earlyEventRepository == null) {
            return;
        }
        for (SagaEarlyEvent staged : earlyEventRepository.findByOrderIdForUpdate(saga.getOrderId())) {
            applyEarlyEventLocked(saga, staged);
        }
    }

    private void applyEarlyEventLocked(SagaInstance saga, SagaEarlyEvent staged) {
        if (!claimInbound(staged.getTopic(), saga.getOrderId(), staged.getPayload())) {
            earlyEventRepository.delete(staged);
            return;
        }
        switch (staged.getTopic()) {
            case "order.cancelled" -> applyOrderCancelledLocked(saga, staged.getPayload());
            case "restaurant.order-confirmed" -> applyRestaurantConfirmedLocked(saga, staged.getPayload());
            default -> throw new IllegalArgumentException("Unsupported staged Saga topic: " + staged.getTopic());
        }
        earlyEventRepository.delete(staged);
        log.info("[Saga] Applied staged {} for orderId={} eventId={}",
                staged.getTopic(), saga.getOrderId(), staged.getEventId());
    }

    private void stageEarlyEvent(String topic, Long orderId, String rawEvent) {
        if (earlyEventRepository == null) {
            throw new IllegalStateException("Early Saga event staging is unavailable for orderId=" + orderId);
        }
        try {
            JsonNode event = objectMapper.readTree(rawEvent);
            JsonNode id = event.get("eventId");
            if (id == null || !id.isTextual()) {
                throw new IllegalArgumentException("Saga early eventId is required");
            }
            UUID eventId = UUID.fromString(id.asText());
            String fingerprint = sha256(rawEvent);
            SagaEarlyEvent existing = earlyEventRepository.findById(eventId).orElse(null);
            if (existing == null) {
                if (insertEarlyEventIfAbsent(eventId, topic, orderId, rawEvent, fingerprint) == 1) {
                    log.info("[Saga] Staged {} before Saga creation for orderId={}, eventId={}",
                            topic, orderId, eventId);
                    return;
                }
                existing = earlyEventRepository.findById(eventId).orElseThrow(() ->
                        new IllegalStateException("Saga early-event conflict resolved without a committed row"));
            }
            requireExactEarlyReplay(existing, topic, orderId, fingerprint);
            log.info("[Saga] Exact early {} replay staged for orderId={}, eventId={}",
                    topic, orderId, eventId);
        } catch (IllegalArgumentException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to stage early Saga event", exception);
        }
    }

    private boolean matchesTimeoutObservation(SagaInstance saga, SagaTimeoutCommand command) {
        if (saga.getStatus() != command.expectedStatus()
                || versionOf(saga) != command.expectedVersion()
                || saga.getUpdatedAt() == null) {
            return false;
        }
        return saga.getUpdatedAt().equals(command.observedAt());
    }

    private long versionOf(SagaInstance saga) {
        return saga.getVersion() == null ? 0L : saga.getVersion();
    }

    /**
     * A malformed persisted offer is treated as due so the timeout command can
     * fail closed through the normal compensation path instead of staying
     * invisible in the scheduler forever.
     */
    private boolean isShipperOfferTimeoutDue(SagaInstance saga) {
        CaseHistory.Fact foundStep = history(saga).latestFact("SHIPPER_FOUND");
        if (foundStep == null || foundStep.eventData() == null) {
            return true;
        }
        try {
            JsonNode foundPayload = objectMapper.readTree(foundStep.eventData());
            Integer waitingTimeoutSeconds = foundPayload.hasNonNull("waitingTimeoutSeconds")
                    ? foundPayload.get("waitingTimeoutSeconds").asInt()
                    : null;
            LocalDateTime offerFoundAt = foundPayload.hasNonNull("foundAt")
                    ? LocalDateTime.parse(foundPayload.get("foundAt").asText())
                    : foundStep.executedAt();
            return ShipperOffer.isDue(offerFoundAt, waitingTimeoutSeconds, LocalDateTime.now());
        } catch (Exception malformed) {
            return true;
        }
    }

    private SagaInstance findSagaByOrderId(Long orderId) {
        if (orderId == null) {
            throw new IllegalArgumentException("orderId is required");
        }
        return sagaInstanceRepository.findByOrderIdForUpdate(orderId)
                .orElseThrow(() -> new IllegalStateException("No saga found for orderId=" + orderId));
    }

    private boolean hasRejectedShipper(SagaInstance saga, Long shipperId) {
        return history(saga).rejectingShippers().contains(shipperId);
    }

    private boolean sameJson(String left, String right) {
        if (left == null || right == null) return false;
        try {
            return objectMapper.readTree(left).equals(objectMapper.readTree(right));
        } catch (Exception malformed) {
            return false;
        }
    }

    private void requireDeliveryIdentity(SagaInstance saga, Long deliveryId, Long orderId) {
        if (deliveryId == null || deliveryId <= 0) {
            throw new IllegalArgumentException("deliveryId must be positive for orderId=" + orderId);
        }
        if (saga.getDeliveryId() == null || !saga.getDeliveryId().equals(deliveryId)) {
            throw new IllegalStateException("Delivery identity mismatch for orderId=" + orderId
                    + ": expected=" + saga.getDeliveryId() + ", received=" + deliveryId);
        }
    }

    /** Kiểm tra saga đã có một step theo tên chưa. */
    private boolean hasStep(SagaInstance saga, String stepName) {
        return history(saga).has(stepName);
    }

    /** Lấy eventData của step gần nhất theo tên (null nếu không có). */
    private String getStepEventData(SagaInstance saga, String stepName) {
        return history(saga).latest(stepName);
    }

    /**
     * Every Find command carries a Saga-owned generation that is independent
     * of the outbox event ID. The persisted MATCHING_STARTED step is the
     * authoritative target for later stop-matching and stale-result fences.
     */
    private String dispatchFindShipperCommand(
            SagaInstance saga,
            Long orderId,
            ObjectNode payload) throws Exception {
        UUID matchingSessionId = nextMatchingSessionId(saga);
        payload.put("matchingSessionId", matchingSessionId.toString());
        String command = objectMapper.writeValueAsString(payload);
        sendCommand(CMD_FIND_SHIPPER, orderId.toString(), command);
        saga.addStep("MATCHING_STARTED", CMD_FIND_SHIPPER, command);
        return command;
    }

    private UUID nextMatchingSessionId(SagaInstance saga) {
        long started = history(saga).count("MATCHING_STARTED");
        return MatchingSession.next(MatchingSession.caseIdentity(saga.getId(), saga.getOrderId()), started);
    }

    private UUID currentMatchingSessionId(SagaInstance saga) {
        // Pre-contract cases yield null so no broad stop can cancel a later rematch.
        return history(saga).currentMatchingSession();
    }

    private boolean isCurrentMatchingResult(SagaInstance saga, String rawEvent) {
        UUID expected = currentMatchingSessionId(saga);
        if (expected == null) {
            // A Saga begun before the generation contract has no safe expected
            // value. Preserve its in-flight compatibility during rollout; new
            // matching attempts always persist the explicit session above.
            return true;
        }
        try {
            JsonNode result = objectMapper.readTree(rawEvent);
            if (!result.hasNonNull("matchingSessionId")) {
                throw new IllegalArgumentException(
                        "Match result matchingSessionId is required for a generation-aware Saga");
            }
            return MatchingSession.isCurrent(expected, UUID.fromString(result.get("matchingSessionId").asText()));
        } catch (IllegalArgumentException exception) {
            throw exception;
        } catch (Exception malformed) {
            throw new IllegalArgumentException("Match result matchingSessionId is malformed", malformed);
        }
    }

    private void sendStopMatchingCommand(SagaInstance saga, Long orderId, String causeEvent) {
        UUID matchingSessionId = currentMatchingSessionId(saga);
        if (matchingSessionId == null) {
            log.info("[Saga] No generation-scoped Match command exists for orderId={}; skipping stop-matching",
                    orderId);
            return;
        }
        if (saga.getDeliveryId() == null || saga.getDeliveryId() <= 0) {
            throw new IllegalStateException(
                    "Cannot stop matching generation without a persisted deliveryId for orderId=" + orderId);
        }
        try {
            ObjectNode stop = objectMapper.createObjectNode();
            stop.put("orderId", orderId);
            stop.put("deliveryId", saga.getDeliveryId());
            stop.put("matchingSessionId", matchingSessionId.toString());
            JsonNode parsedCause = objectMapper.readTree(causeEvent);
            if (parsedCause.hasNonNull("eventId")) {
                stop.put("causeEventId", parsedCause.get("eventId").asText());
            }
            sendCommand(CMD_STOP_MATCHING, orderId.toString(), objectMapper.writeValueAsString(stop));
        } catch (SagaCommandPublishException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot build generation-scoped stop-matching command", exception);
        }
    }

    /**
     * Every matching attempt is rebuilt from Saga-owned canonical state. Rejection
     * and timeout events are control signals, not authorities for price/payment or
     * delivery coordinates.
     */
    private ObjectNode buildFindShipperPayload(SagaInstance saga, String attemptEvent) throws Exception {
        JsonNode parsedAttempt = objectMapper.readTree(attemptEvent);
        if (!(parsedAttempt instanceof ObjectNode)) {
            throw new IllegalArgumentException("Find-shipper payload must be a JSON object");
        }
        String deliveryData = getStepEventData(saga, "DELIVERY_CREATED");
        String matchingStartData = getStepEventData(saga, "MATCHING_STARTED");
        Map<String, Object> fields = MatchingCommandAssembly.assemble(
                facts(parsedAttempt),
                deliveryData == null ? null : facts(objectMapper.readTree(deliveryData)),
                matchingStartData == null ? null : facts(objectMapper.readTree(matchingStartData)),
                facts(objectMapper.readTree(saga.getPayload())),
                batchClientCapabilityEnabled);
        ObjectNode payload = objectMapper.createObjectNode();
        fields.forEach((field, value) -> {
            if (value instanceof JsonNode node) {
                payload.set(field, node);
            } else {
                payload.put(field, (Boolean) value);
            }
        });

        MatchingCommandPolicy.requireCanonical(payload.hasNonNull("orderId"), payload.hasNonNull("deliveryId"),
                payload.hasNonNull("paymentMethod") ? payload.get("paymentMethod").asText() : null,
                payload.hasNonNull("totalPrice") ? payload.get("totalPrice").decimalValue() : null);
        return payload;
    }

    /** JSON view used by the domain assembly; a non-object source contributes no facts. */
    private static MatchingCommandAssembly.Facts facts(JsonNode source) {
        if (source == null || !source.isObject()) return null;
        return new MatchingCommandAssembly.Facts() {
            @Override public boolean hasNonNull(String field) { return source.hasNonNull(field); }
            @Override public Object get(String field) { return source.get(field); }
        };
    }

    /**
     * Claims a Kafka event before any Saga mutation or command-outbox write. The
     * primary-key claim commits with the Saga mutation/outbox so a concurrent
     * duplicate cannot perform a side effect and a conflicting replay is never
     * silently acknowledged.
     */
    private boolean claimInbound(String topic, Long orderId, String rawEvent) {
        if (inboundReceiptRepository == null) return true;
        try {
            JsonNode event = objectMapper.readTree(rawEvent);
            JsonNode id = event.get("eventId");
            if (id == null || !id.isTextual()) {
                throw new IllegalArgumentException("Saga inbound eventId is required");
            }
            UUID eventId = UUID.fromString(id.asText());
            String fingerprint = sha256(rawEvent);
            var existing = inboundReceiptRepository.findById(eventId).orElse(null);
            if (existing == null) {
                if (insertInboundReceiptIfAbsent(eventId, topic, orderId, fingerprint) == 1) {
                    return true;
                }
                existing = inboundReceiptRepository.findById(eventId).orElseThrow(() ->
                        new IllegalStateException("Saga inbound receipt conflict resolved without a committed row"));
            }
            requireExactInboundReplay(existing, topic, orderId, fingerprint);
            log.info("[Saga] Exact inbound replay eventId={}, topic={}, orderId={}, skipping",
                    eventId, topic, orderId);
            return false;
        } catch (IllegalArgumentException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to claim Saga inbound event", exception);
        }
    }

    private int insertInboundReceiptIfAbsent(UUID eventId, String topic, Long orderId, String fingerprint) {
        if (dataSourceUrl != null && dataSourceUrl.startsWith("jdbc:h2:")) {
            return inboundReceiptRepository.insertIfAbsentH2(eventId, topic, orderId, fingerprint);
        }
        return inboundReceiptRepository.insertIfAbsentPostgres(eventId, topic, orderId, fingerprint);
    }

    private int insertEarlyEventIfAbsent(UUID eventId, String topic, Long orderId,
                                         String payload, String fingerprint) {
        if (dataSourceUrl != null && dataSourceUrl.startsWith("jdbc:h2:")) {
            return earlyEventRepository.insertIfAbsentH2(eventId, topic, orderId, payload, fingerprint);
        }
        return earlyEventRepository.insertIfAbsentPostgres(eventId, topic, orderId, payload, fingerprint);
    }

    private void requireExactInboundReplay(
            com.delivery.saga_orchestrator_service.entity.SagaInboundReceipt existing,
            String topic, Long orderId, String fingerprint) {
        if (!existing.getTopic().equals(topic) || !existing.getOrderId().equals(orderId)
                || !existing.getPayloadFingerprint().equals(fingerprint)) {
            throw new IllegalArgumentException("Saga eventId replay has a contradictory payload");
        }
    }

    private void requireExactEarlyReplay(SagaEarlyEvent existing, String topic, Long orderId, String fingerprint) {
        if (!existing.getTopic().equals(topic) || !existing.getOrderId().equals(orderId)
                || !existing.getPayloadFingerprint().equals(fingerprint)) {
            throw new IllegalArgumentException("Saga early eventId replay has a contradictory payload");
        }
    }

    private String sha256(String payload) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(payload.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(64);
            for (byte value : digest) hex.append(String.format("%02x", value));
            return hex.toString();
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private UUID sendCommand(String topic, String key, String payload) {
        try {
            JsonNode jsonNode = objectMapper.readTree(payload);
            String destination = resolveCommandTopic(topic);
            return outboxService.saveCommand(key, destination, key, jsonNode);
        } catch (Exception e) {
            log.error("💥 [Saga] Failed to store command for {}: {}", topic, e.getMessage(), e);
            throw new SagaCommandPublishException("Failed to store saga command for " + topic, e);
        }
    }

    /**
     * Gửi lệnh update order status (bọc thêm trường sagaStatus)
     */
    private void sendOrderStatusCommand(Long orderId, String sagaStatus, String rawEvent) {
        sendOrderStatusCommand(findSagaByOrderId(orderId), sagaStatus, rawEvent);
    }

    private void sendOrderStatusCommand(SagaInstance saga, String sagaStatus, String rawEvent) {
        try {
            Long orderId = saga.getOrderId();
            ObjectNode command = objectMapper.createObjectNode();
            command.put("orderId", orderId);
            command.put("sagaStatus", sagaStatus);
            command.put("orderStatusSequence", saga.getOrderStatusSequence() + 1);
            command.put("originalEvent", rawEvent);
            command.put("timestamp", System.currentTimeMillis());
            saga.setOrderStatusSequence(saga.getOrderStatusSequence() + 1);
            outboxService.saveCommand(orderId.toString(), updateOrderStatusTopic,
                    orderId.toString(), command);
        } catch (Exception e) {
            log.error("💥 [Saga] Failed to store order status command: {}", e.getMessage(), e);
            throw new SagaCommandPublishException("Failed to store saga order status command", e);
        }
    }

    private String resolveCommandTopic(String canonicalTopic) {
        return switch (canonicalTopic) {
            case CMD_CREATE_DELIVERY -> createDeliveryTopic;
            case CMD_CANCEL_DELIVERY -> cancelDeliveryTopic;
            case CMD_FIND_SHIPPER -> findShipperTopic;
            case CMD_CACHE_SHIPPER_FOUND -> cacheShipperFoundTopic;
            case CMD_EXPIRE_SHIPPER_OFFER -> expireShipperOfferTopic;
            case CMD_MARK_SHIPPER_NOT_FOUND -> markShipperNotFoundTopic;
            case CMD_STOP_MATCHING -> stopMatchingTopic;
            case CMD_UPDATE_ORDER_STATUS -> updateOrderStatusTopic;
            default -> throw new IllegalArgumentException("Unsupported Saga command topic: " + canonicalTopic);
        };
    }

    private static DispatchStatus dispatch(SagaInstance saga) {
        return DispatchStatus.valueOf(saga.getStatus().name());
    }

    private static void putRetrySettings(ObjectNode payload, MatchingRetrySettings settings) {
        payload.put("maxRetryAttempts", settings.maxRetryAttempts());
        payload.put("initialDelaySeconds", settings.initialDelaySeconds());
        payload.put("maxDelaySeconds", settings.maxDelaySeconds());
        payload.put("backoffMultiplier", settings.backoffMultiplier());
    }

    private CaseHistory history(SagaInstance saga) {
        return new JsonCaseHistory(saga, objectMapper);
    }

    private static final class SagaCommandPublishException extends RuntimeException {
        private SagaCommandPublishException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
