package com.delivery.dispatch.infrastructure.service;

import com.delivery.dispatch.infrastructure.entity.SagaInstance;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Writes every dispatch command to the transactional outbox. A command is
 * stored in the caller's transaction, so it is published only if the case
 * mutation that produced it commits.
 */
@Slf4j
final class DispatchCommandOutbox {

    private final SagaOutboxService outboxService;
    private final ObjectMapper objectMapper;
    private final UnaryOperator<String> topicResolver;
    private final Supplier<String> orderStatusTopic;

    DispatchCommandOutbox(SagaOutboxService outboxService, ObjectMapper objectMapper,
                          UnaryOperator<String> topicResolver, Supplier<String> orderStatusTopic) {
        this.outboxService = outboxService;
        this.objectMapper = objectMapper;
        this.topicResolver = topicResolver;
        this.orderStatusTopic = orderStatusTopic;
    }

    /** Stores one command under its configured destination; returns the outbox event id. */
    UUID send(String topic, String key, String payload) {
        try {
            JsonNode jsonNode = objectMapper.readTree(payload);
            String destination = topicResolver.apply(topic);
            return outboxService.saveCommand(key, destination, key, jsonNode);
        } catch (Exception e) {
            log.error("💥 [Saga] Failed to store command for {}: {}", topic, e.getMessage(), e);
            throw new DispatchCommandPublishException("Failed to store saga command for " + topic, e);
        }
    }

    /** Order status commands carry a per-case monotonic sequence so Order can drop reordered updates. */
    void orderStatus(SagaInstance saga, String sagaStatus, String rawEvent) {
        try {
            Long orderId = saga.getOrderId();
            ObjectNode command = objectMapper.createObjectNode();
            command.put("orderId", orderId);
            command.put("sagaStatus", sagaStatus);
            command.put("orderStatusSequence", saga.getOrderStatusSequence() + 1);
            command.put("originalEvent", rawEvent);
            command.put("timestamp", System.currentTimeMillis());
            saga.setOrderStatusSequence(saga.getOrderStatusSequence() + 1);
            outboxService.saveCommand(orderId.toString(), orderStatusTopic.get(),
                    orderId.toString(), command);
        } catch (Exception e) {
            log.error("💥 [Saga] Failed to store order status command: {}", e.getMessage(), e);
            throw new DispatchCommandPublishException("Failed to store saga order status command", e);
        }
    }
}
