package com.delivery.match_service.listener;

import com.delivery.match.domain.availability.ShipperProjectionPolicy;

import com.delivery.match_service.repository.MatchRedisGeoRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Map;
import com.delivery.identity.contracts.SimulationContext;
import java.util.UUID;

/**
 * ✅ Kafka Listener để replicate vị trí shipper từ tracking-service
 * Consume 2 topics:
 *   - shipper.location-updated: cập nhật vị trí vào local Redis Geo
 *   - shipper.status-change: cập nhật busy/available flag
 */
@Slf4j
@Component
public class ShipperLocationEventListener {


    private final MatchRedisGeoRepository matchRedisGeoRepository;
    private final ObjectMapper objectMapper;

    public ShipperLocationEventListener(MatchRedisGeoRepository matchRedisGeoRepository) {
        this.matchRedisGeoRepository = matchRedisGeoRepository;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * ✅ Consume vị trí shipper từ tracking-service → GEOADD vào local Redis
     */
    @KafkaListener(topics = "shipper.location-updated", groupId = "${spring.kafka.consumer.group-id:match-service}",
            containerFactory = "locationKafkaListenerContainerFactory",
            autoStartup = "${match.kafka.listener.auto-startup:true}")
    @SuppressWarnings("unchecked")
    public void handleShipperLocationUpdated(String message, Acknowledgment acknowledgment) {
        try {
            Map<String, Object> event = objectMapper.readValue(message, Map.class);

            Long shipperId = ((Number) event.get("shipperId")).longValue();
            Double latitude = event.get("latitude") != null ? ((Number) event.get("latitude")).doubleValue() : null;
            Double longitude = event.get("longitude") != null ? ((Number) event.get("longitude")).doubleValue() : null;
            Boolean isOnline = (Boolean) event.get("isOnline");
            long timestamp = event.get("timestamp") instanceof Number number
                    ? number.longValue() : 0L;
            switch (ShipperProjectionPolicy.onLocation(
                    shipperId, latitude, longitude, isOnline, timestamp, System.currentTimeMillis())) {
                case IGNORE_EXPIRED_ONLINE -> {
                    log.info("Ignoring expired online location replay for shipper {} at {}",
                            shipperId, timestamp);
                    acknowledgment.acknowledge();
                    return;
                }
                case APPLY_ONLINE -> matchRedisGeoRepository.addOrUpdateShipperLocation(
                        shipperId, latitude, longitude, true, timestamp, context(event));
                case APPLY_OFFLINE -> matchRedisGeoRepository.markShipperOffline(shipperId, timestamp, context(event));
            }

            log.debug("📍 Replicated shipper {} location to local Geo", shipperId);
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("💥 Error processing shipper location event: {}", e.getMessage());
            throw new IllegalStateException("Failed to process shipper location event", e);
        }
    }

    private SimulationContext context(Map<String, Object> event) {
        Object raw = event.get("simulationContext");
        if (!(raw instanceof Map<?, ?> values)) return SimulationContext.real();
        try {
            Object mode = values.get("mode");
            if (SimulationContext.ExecutionMode.REAL.name().equals(String.valueOf(mode))) {
                return SimulationContext.real();
            }
            Object run = values.get("runId");
            Object cohort = values.get("cohortId");
            Object version = values.get("bindingVersion");
            SimulationContext context = new SimulationContext(SimulationContext.ExecutionMode.valueOf(String.valueOf(mode)),
                    UUID.fromString(String.valueOf(run)), UUID.fromString(String.valueOf(cohort)),
                    ((Number) version).longValue());
            context.requireValid();
            return context;
        } catch (RuntimeException invalid) {
            throw new IllegalArgumentException("Invalid simulation context in shipper location event", invalid);
        }
    }

    /**
     * ✅ Consume trạng thái busy/available từ delivery-service
     */
    @KafkaListener(topics = "shipper.status-change", groupId = "${spring.kafka.consumer.group-id:match-service}",
            autoStartup = "${match.kafka.listener.auto-startup:true}")
    @SuppressWarnings("unchecked")
    public void handleShipperStatusChange(String message, Acknowledgment acknowledgment) {
        try {
            Map<String, Object> event = objectMapper.readValue(message, Map.class);

            Long shipperId = ((Number) event.get("shipperId")).longValue();
            String status = (String) event.get("status");
            Long deliveryId = event.get("deliveryId") instanceof Number number
                    ? number.longValue() : null;
            Long orderId = event.get("orderId") instanceof Number number
                    ? number.longValue() : null;
            long timestamp = event.get("timestamp") instanceof Number number
                    ? number.longValue() : 0L;
            String eventId = event.get("eventId") instanceof String value ? value : null;
            String canonicalStatus = ShipperProjectionPolicy.canonicalStatus(
                    shipperId, deliveryId, orderId, timestamp, eventId, status);

            log.info("📥 [MatchGeo] Received shipper status: shipper={}, status={}", shipperId, status);

            SimulationContext simulationContext = context(event);
            if (simulationContext.isSimulation()) {
                matchRedisGeoRepository.applyShipperStatus(
                        shipperId, deliveryId, canonicalStatus, timestamp, eventId, simulationContext);
            } else {
                matchRedisGeoRepository.applyShipperStatus(
                        shipperId, deliveryId, canonicalStatus, timestamp, eventId);
            }

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("💥 Error processing shipper status change: {}", e.getMessage());
            throw new IllegalStateException("Failed to process shipper status change", e);
        }
    }
}
