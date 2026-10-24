package com.delivery.dispatch.domain;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Field authority for every find-shipper command. A matching attempt is
 * rebuilt from case-owned facts: rejection and timeout events are control
 * signals, never the authority for money, payment or delivery coordinates.
 * Values are opaque to the domain (the adapter keeps their JSON form), so
 * numeric representation is never altered.
 */
public final class MatchingCommandAssembly {

    /** Fields accepted from the triggering attempt event, in emission order. */
    public static final List<String> ATTEMPT_FIELDS = List.of(
            "orderId", "deliveryId", "pickupAddress", "pickupLat", "pickupLng",
            "deliveryAddress", "deliveryLat", "deliveryLng", "totalPrice",
            "shippingFee", "paymentMethod", "restaurantId", "restaurantName", "matchingDeadlineAt",
            "simulationContext", "batchOfferEnabled", "batchWave");

    /** Delivery-owned location facts override the attempt. */
    public static final List<String> DELIVERY_FIELDS = List.of(
            "deliveryId", "pickupAddress", "pickupLat", "pickupLng",
            "deliveryAddress", "deliveryLat", "deliveryLng");

    /** Every rematch keeps the absolute deadline of the first generation. */
    public static final List<String> MATCHING_START_FIELDS = List.of("matchingDeadlineAt");

    /** Order-owned money, payment and restaurant facts override everything else. */
    public static final List<String> ORDER_FIELDS = List.of(
            "orderId", "totalPrice", "shippingFee", "paymentMethod",
            "restaurantId", "restaurantName", "simulationContext");

    public static final String BATCH_OFFER_ENABLED = "batchOfferEnabled";

    /** Read-only view of one fact source; absent and JSON-null values are both "missing". */
    public interface Facts {
        boolean hasNonNull(String field);

        Object get(String field);
    }

    private MatchingCommandAssembly() {
    }

    /**
     * @param delivery      DELIVERY_CREATED facts, or null before Delivery exists
     * @param matchingStart latest MATCHING_STARTED facts, or null for the first generation
     * @param batchCapability whether the client batch capability is enabled (always overrides)
     * @return ordered command fields; insertion order follows first appearance
     */
    public static Map<String, Object> assemble(Facts attempt, Facts delivery, Facts matchingStart,
                                               Facts order, boolean batchCapability) {
        Map<String, Object> command = new LinkedHashMap<>();
        copy(attempt, command, ATTEMPT_FIELDS);
        command.put(BATCH_OFFER_ENABLED, batchCapability);
        copy(delivery, command, DELIVERY_FIELDS);
        copy(matchingStart, command, MATCHING_START_FIELDS);
        copy(order, command, ORDER_FIELDS);
        return command;
    }

    private static void copy(Facts source, Map<String, Object> target, List<String> fields) {
        if (source == null) return;
        for (String field : fields) {
            if (source.hasNonNull(field)) {
                target.put(field, source.get(field));
            }
        }
    }
}
