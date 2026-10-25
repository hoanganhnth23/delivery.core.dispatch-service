package com.delivery.dispatch.infrastructure.service;

import com.delivery.dispatch.domain.RematchPolicy;
import com.delivery.dispatch.domain.MatchingRetrySettings;
import com.delivery.dispatch.domain.OfferRetirementPolicy;
import com.delivery.dispatch.infrastructure.entity.SagaInstance;
import com.delivery.dispatch.infrastructure.entity.SagaInstance.SagaStatus;
import com.delivery.dispatch.infrastructure.entity.SagaEarlyEvent;
import com.delivery.dispatch.infrastructure.repository.SagaInstanceRepository;
import com.delivery.dispatch.infrastructure.repository.SagaInboundReceiptRepository;
import com.delivery.dispatch.infrastructure.repository.SagaEarlyEventRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Dispatch coordinator: one dispatch case per order, from creation to delivery.
 *
 * <p>Each public handler locks the case, claims the inbound event
 * ({@link DispatchInbox}), lets a pure use case decide ({@link DispatchUseCases}),
 * then persists the case and its outbox commands in the same transaction.
 * Match commands are built by {@link MatchingCommands}; offer expiry is
 * {@link OfferExpiry}. Topic names keep the historical {@code saga.command.*}
 * contract shared with Order, Delivery and Match.
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
public class DispatchCoordinator {

    private final SagaInstanceRepository sagaInstanceRepository;
    private final ObjectMapper objectMapper;
    private final DispatchInbox inbox;
    private final DispatchCommandOutbox outbox;
    private final MatchingCommands matching;
    private final OfferExpiry offerExpiry;
    private final DispatchUseCases useCases;

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
    @Value("${app.saga.timeout.finding-shipper-minutes:15}")
    private int findingShipperTimeoutMinutes = 15;
    @Value("${app.dispatch.max-failed-offers:" + RematchPolicy.DEFAULT_MAX_FAILED_OFFERS + "}")
    private int maxFailedOffers = RematchPolicy.DEFAULT_MAX_FAILED_OFFERS;
    @Value("${matching.batch.client-capability-enabled:false}")
    private boolean batchClientCapabilityEnabled;
    @Value("${spring.datasource.url:}")
    private String dataSourceUrl;

    @Autowired
    public DispatchCoordinator(SagaInstanceRepository sagaInstanceRepository,
                               SagaOutboxService outboxService,
                               SagaInboundReceiptRepository inboundReceiptRepository,
                               SagaEarlyEventRepository earlyEventRepository) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.objectMapper = new ObjectMapper();
        // Collaborators read configuration lazily: @Value fields are injected after construction.
        this.inbox = new DispatchInbox(inboundReceiptRepository, earlyEventRepository, objectMapper,
                () -> dataSourceUrl);
        this.outbox = new DispatchCommandOutbox(outboxService, objectMapper, this::resolveCommandTopic,
                () -> updateOrderStatusTopic);
        this.matching = new MatchingCommands(objectMapper, outbox, () -> batchClientCapabilityEnabled);
        this.offerExpiry = new OfferExpiry(objectMapper, outbox, matching, sagaInstanceRepository::save,
                this::handleStepFailedLocked, () -> maxFailedOffers);
        this.useCases = new DispatchUseCases(sagaInstanceRepository::save, this::findSagaByOrderId,
                objectMapper, outbox, matching, this::sameJson,
                () -> new MatchingRetrySettings(initialMatchMaxRetryAttempts, initialMatchDelaySeconds,
                        initialMatchMaxDelaySeconds, initialMatchBackoffMultiplier),
                () -> findingShipperTimeoutMinutes, () -> maxFailedOffers);
    }

    /** Compatibility constructor for tests that exercise the durable inbox but not early-event staging. */
    public DispatchCoordinator(SagaInstanceRepository sagaInstanceRepository,
                               SagaOutboxService outboxService,
                               SagaInboundReceiptRepository inboundReceiptRepository) {
        this(sagaInstanceRepository, outboxService, inboundReceiptRepository, null);
    }

    /** Compatibility constructor for focused tests that do not exercise the inbox boundary. */
    public DispatchCoordinator(SagaInstanceRepository sagaInstanceRepository, SagaOutboxService outboxService) {
        this(sagaInstanceRepository, outboxService, null, null);
    }

    // ==================== EVENT HANDLERS ====================

    /**
     * Step 1: order.created → Tạo saga + phát lệnh tạo delivery
     */
    @Transactional
    public void handleOrderCreated(Long orderId, String rawEvent) {
        if (!inbox.claim("order.created", orderId, rawEvent)) return;
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
        outbox.send(CMD_CREATE_DELIVERY, orderId.toString(), rawEvent);
        log.info("📤 [Saga] Sent command: {} for orderId={}", CMD_CREATE_DELIVERY, orderId);
    }

    /**
     * Step 2: delivery.created.result → Delivery đã tạo.
     * ✅ GATE: KHÔNG tìm shipper ngay. Chỉ tìm shipper SAU KHI nhà hàng confirm đơn
     * (restaurant.order-confirmed). Nếu nhà hàng đã confirm trước đó (race) thì tìm luôn.
     */
    @Transactional
    public void handleDeliveryCreated(Long orderId, Long deliveryId, String rawEvent) {
        if (!inbox.claim("delivery.created.result", orderId, rawEvent)) return;
        SagaInstance saga = findSagaByOrderId(orderId);
        switch (useCases.deliveryCreation().onDeliveryCreated(new SagaDispatchCase(saga, objectMapper), deliveryId, rawEvent)) {
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
            inbox.stage("restaurant.order-confirmed", orderId, rawEvent);
            return;
        }
        if (!inbox.claim("restaurant.order-confirmed", orderId, rawEvent)) return;
        applyRestaurantConfirmedLocked(saga, rawEvent);
    }

    private void applyRestaurantConfirmedLocked(SagaInstance saga, String rawEvent) {
        Long orderId = saga.getOrderId();
        switch (useCases.orderLifecycle().confirmRestaurant(new SagaDispatchCase(saga, objectMapper), rawEvent)) {
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
     * Step 3a: shipper.found → request Delivery persist the offer. Order must
     * remain FINDING_SHIPPER until Delivery confirms its own transaction.
     */
    @Transactional
    public void handleShipperFound(Long orderId, Long deliveryId, String rawEvent) {
        if (!inbox.claim("shipper.found", orderId, rawEvent)) return;
        SagaInstance saga = findSagaByOrderId(orderId);
        requireDeliveryIdentity(saga, deliveryId, orderId);
        switch (useCases.matchOutcomes().onShipperFound(new SagaDispatchCase(saga, objectMapper),
                () -> matching.isCurrentResult(saga, rawEvent), rawEvent)) {
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
        if (!inbox.claim("delivery.offer-persisted", orderId, rawEvent)) return;
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
        switch (useCases.matchOutcomes().onOfferPersisted(new SagaDispatchCase(saga, objectMapper), sourceCommandId,
                () -> matching.isCurrentResult(saga, rawEvent), rawEvent)) {
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
        if (!inbox.claim("shipper.not-found", orderId, rawEvent)) return;
        SagaInstance saga = findSagaByOrderId(orderId);
        requireDeliveryIdentity(saga, deliveryId, orderId);
        switch (useCases.matchOutcomes().onShipperNotFound(new SagaDispatchCase(saga, objectMapper),
                () -> matching.isCurrentResult(saga, rawEvent), rawEvent)) {
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
        if (!inbox.claim("delivery.shipper-accepted", orderId, rawEvent)) return;
        SagaInstance saga = findSagaByOrderId(orderId);
        requireDeliveryIdentity(saga, deliveryId, orderId);
        // Acceptance and rejection travel on different topics; a delayed acceptance from an
        // excluded shipper must not resurrect it, while one overtaking an offer timeout may assign.
        switch (useCases.assignments().onAccepted(new SagaDispatchCase(saga, objectMapper), shipperId, rawEvent)) {
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
        if (!inbox.claim("delivery.shipper-rejected", orderId, rawEvent)) return;
        SagaInstance saga = findSagaByOrderId(orderId);
        requireDeliveryIdentity(saga, deliveryId, orderId);
        // Pre-accept rejection and post-accept cancel-assignment both rematch.
        switch (useCases.assignments().onRejected(new SagaDispatchCase(saga, objectMapper), rejectedShipperId, rawEvent)) {
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
        if (!OfferExpiry.matchesObservation(saga, command)) {
            log.info("[Saga] Ignoring stale timeout for orderId={} expected={}/{} observed={}/{}",
                    command.orderId(), command.expectedStatus(), command.expectedVersion(),
                    saga.getStatus(), OfferExpiry.versionOf(saga));
            return;
        }

        // A shipper-offer candidate is intentionally queried from a very small
        // minimum age. Do not consume an inbox receipt until its exact offer
        // deadline has actually elapsed.
        if (command.expectedStatus() == SagaStatus.SHIPPER_FOUND) {
            if (!offerExpiry.isDue(saga)) {
                return;
            }
        } else if (LocalDateTime.now().isBefore(command.deadline())) {
            log.info("[Saga] Ignoring early timeout for orderId={} status={} deadline={}",
                    command.orderId(), command.expectedStatus(), command.deadlineAt());
            return;
        }

        String rawTimeoutEvent = command.toJson(objectMapper);
        if (!inbox.claim("saga.timeout." + command.expectedStatus().name(),
                command.orderId(), rawTimeoutEvent)) {
            return;
        }

        if (command.expectedStatus() == SagaStatus.SHIPPER_FOUND) {
            offerExpiry.expire(saga, rawTimeoutEvent);
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
     * delivery.offer-retired → Delivery acknowledged the expire command. Only
     * the acknowledgement of the currently requested expiry may advance the case.
     */
    @Transactional
    public void handleOfferRetired(Long orderId, Long deliveryId, String rawEvent) {
        if (!inbox.claim("delivery.offer-retired", orderId, rawEvent)) return;
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
        switch (useCases.matchOutcomes().onOfferRetired(new SagaDispatchCase(saga, objectMapper), sourceCommandId,
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
        if (!inbox.claim("delivery.status-updated", orderId, rawEvent)) return;
        SagaInstance saga = findSagaByOrderId(orderId);
        requireDeliveryIdentity(saga, deliveryId, orderId);

        SagaDispatchCase dispatchCase = new SagaDispatchCase(saga, objectMapper);
        switch (useCases.deliveryProgress().apply(dispatchCase, newStatus, rawEvent)) {
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

    /**
     * order.cancelled → Compensation: cancel delivery + stop matching
     */
    @Transactional
    public void handleOrderCancelled(Long orderId, String rawEvent) {
        SagaInstance saga = sagaInstanceRepository.findByOrderIdForUpdate(orderId).orElse(null);
        if (saga == null) {
            inbox.stage("order.cancelled", orderId, rawEvent);
            return;
        }
        if (!inbox.claim("order.cancelled", orderId, rawEvent)) return;
        applyOrderCancelledLocked(saga, rawEvent);
    }

    private void applyOrderCancelledLocked(SagaInstance saga, String rawEvent) {
        Long orderId = saga.getOrderId();
        switch (useCases.orderLifecycle().cancelOrder(new SagaDispatchCase(saga, objectMapper), rawEvent)) {
            case REPLAY -> log.info("[Saga] Exact order-cancelled replay for orderId={}, skipping", orderId);
            case IGNORED_FAILED -> log.info("Ignoring cancellation consequence for failed Saga orderId={}", orderId);
            case CANCELLED, COMPENSATING ->
                    log.warn("🚨 [Saga] COMPENSATION — order cancelled, orderId={}", orderId);
        }
    }

    // ==================== FAILURE HANDLERS ====================

    /**
     * ❌ delivery.created.failed → Tạo delivery thất bại → báo Order cancel
     */
    @Transactional
    public void handleDeliveryCreationFailed(Long orderId, String reason, String rawEvent) {
        if (!inbox.claim("delivery.created.failed", orderId, rawEvent)) return;
        SagaInstance saga = findSagaByOrderId(orderId);
        switch (useCases.deliveryCreation().onDeliveryCreationFailed(new SagaDispatchCase(saga, objectMapper), rawEvent)) {
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
        if (!inbox.claim(stepName + ".failed", orderId, rawEvent)) return;
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
        switch (useCases.stepFailures().onStepFailed(new SagaDispatchCase(saga, objectMapper), stepName, rawEvent)) {
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
        if (!inbox.stagingAvailable() || eventId == null) {
            return;
        }
        SagaEarlyEvent observed = inbox.find(eventId);
        if (observed == null) {
            return;
        }
        SagaInstance saga = sagaInstanceRepository.findByOrderIdForUpdate(observed.getOrderId()).orElse(null);
        if (saga == null) {
            return;
        }
        SagaEarlyEvent staged = inbox.lock(eventId);
        if (staged == null) {
            return;
        }
        applyEarlyEventLocked(saga, staged);
    }

    // ==================== HELPERS ====================

    private void drainEarlyEventsForSaga(SagaInstance saga) {
        if (!inbox.stagingAvailable()) {
            return;
        }
        for (SagaEarlyEvent staged : inbox.lockStaged(saga.getOrderId())) {
            applyEarlyEventLocked(saga, staged);
        }
    }

    private void applyEarlyEventLocked(SagaInstance saga, SagaEarlyEvent staged) {
        if (!inbox.claim(staged.getTopic(), saga.getOrderId(), staged.getPayload())) {
            inbox.discard(staged);
            return;
        }
        switch (staged.getTopic()) {
            case "order.cancelled" -> applyOrderCancelledLocked(saga, staged.getPayload());
            case "restaurant.order-confirmed" -> applyRestaurantConfirmedLocked(saga, staged.getPayload());
            default -> throw new IllegalArgumentException("Unsupported staged Saga topic: " + staged.getTopic());
        }
        inbox.discard(staged);
        log.info("[Saga] Applied staged {} for orderId={} eventId={}",
                staged.getTopic(), saga.getOrderId(), staged.getEventId());
    }

    private SagaInstance findSagaByOrderId(Long orderId) {
        if (orderId == null) {
            throw new IllegalArgumentException("orderId is required");
        }
        return sagaInstanceRepository.findByOrderIdForUpdate(orderId)
                .orElseThrow(() -> new IllegalStateException("No saga found for orderId=" + orderId));
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
}
