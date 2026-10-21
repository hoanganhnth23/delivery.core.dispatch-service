package com.delivery.match_service.listener;

import com.delivery.match.domain.availability.ShipperProjectionPolicy;

import com.delivery.identity.contracts.SimulationContext;
import com.delivery.match_service.service.CompletedDeliveryProjection;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;

/** Updates the Match-owned fairness input from the canonical delivery completion event. */
@Slf4j
@Component
public class DeliveryCompletedProjectionListener {
    private final CompletedDeliveryProjection projection;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public DeliveryCompletedProjectionListener(CompletedDeliveryProjection projection) {
        this.projection = projection;
    }

    @KafkaListener(topics = "delivery.completed", groupId = "${spring.kafka.consumer.group-id:match-service}",
            autoStartup = "${match.kafka.listener.auto-startup:true}")
    @SuppressWarnings("unchecked")
    public void handleDeliveryCompleted(String message, Acknowledgment acknowledgment) {
        try {
            Map<String, Object> event = objectMapper.readValue(message, Map.class);
            UUID eventId = UUID.fromString(String.valueOf(event.get("eventId")));
            long shipperId = ShipperProjectionPolicy.completedDeliveryShipper(event.get("shipperId"));
            boolean recorded = projection.record(eventId, shipperId, context(event));
            log.debug("Projected delivery.completed {} for shipper {} (new={})", eventId, shipperId, recorded);
            acknowledgment.acknowledge();
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to project delivery.completed for Match", exception);
        }
    }

    private SimulationContext context(Map<String, Object> event) {
        Object raw = event.get("simulationContext");
        if (!(raw instanceof Map<?, ?> values)) return SimulationContext.real();
        try {
            if (SimulationContext.ExecutionMode.REAL.name().equals(String.valueOf(values.get("mode")))) {
                return SimulationContext.real();
            }
            SimulationContext context = new SimulationContext(
                    SimulationContext.ExecutionMode.valueOf(String.valueOf(values.get("mode"))),
                    UUID.fromString(String.valueOf(values.get("runId"))),
                    UUID.fromString(String.valueOf(values.get("cohortId"))),
                    ((Number) values.get("bindingVersion")).longValue());
            context.requireValid();
            return context;
        } catch (RuntimeException invalid) {
            throw new IllegalArgumentException("Invalid simulation context in delivery.completed", invalid);
        }
    }
}
