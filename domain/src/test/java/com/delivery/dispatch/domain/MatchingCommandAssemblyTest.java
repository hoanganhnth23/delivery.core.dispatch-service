package com.delivery.dispatch.domain;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MatchingCommandAssemblyTest {

    private static MatchingCommandAssembly.Facts facts(Map<String, Object> values) {
        return new MatchingCommandAssembly.Facts() {
            @Override public boolean hasNonNull(String field) { return values.get(field) != null; }
            @Override public Object get(String field) { return values.get(field); }
        };
    }

    @Test
    void caseOwnedFactsOverrideTheAttemptInAuthorityOrder() {
        Map<String, Object> attempt = new HashMap<>();
        attempt.put("orderId", 1);
        attempt.put("deliveryId", 2);
        attempt.put("pickupLat", "attempt-lat");
        attempt.put("totalPrice", "attempt-price");
        attempt.put("matchingDeadlineAt", "attempt-deadline");
        attempt.put("batchOfferEnabled", true);
        attempt.put("batchWave", 2);
        attempt.put("rejectedShipperId", 9);
        attempt.put("restaurantName", null);
        Map<String, Object> delivery = Map.of("pickupLat", "delivery-lat", "totalPrice", "ignored");
        Map<String, Object> start = Map.of("matchingDeadlineAt", "first-deadline");
        Map<String, Object> order = Map.of("totalPrice", "order-price", "paymentMethod", "COD",
                "restaurantName", "Quán");

        Map<String, Object> command = MatchingCommandAssembly.assemble(facts(attempt), facts(delivery),
                facts(start), facts(order), false);

        assertEquals(List.of("orderId", "deliveryId", "pickupLat", "totalPrice", "matchingDeadlineAt",
                        "batchOfferEnabled", "batchWave", "paymentMethod", "restaurantName"),
                new ArrayList<>(command.keySet()));
        assertEquals("delivery-lat", command.get("pickupLat"));
        assertEquals("order-price", command.get("totalPrice"));
        assertEquals("first-deadline", command.get("matchingDeadlineAt"));
        assertEquals(false, command.get("batchOfferEnabled"));
        assertEquals("Quán", command.get("restaurantName"));
        assertFalse(command.containsKey("rejectedShipperId"));
    }

    @Test
    void missingSourcesContributeNothingAndCapabilityIsAlwaysSet() {
        Map<String, Object> command = MatchingCommandAssembly.assemble(facts(Map.of("orderId", 1)), null, null,
                facts(Map.of()), true);
        assertEquals(Map.of("orderId", 1, "batchOfferEnabled", true), command);
    }
}
