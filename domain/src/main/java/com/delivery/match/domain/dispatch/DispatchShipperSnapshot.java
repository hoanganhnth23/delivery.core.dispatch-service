package com.delivery.match.domain.dispatch;

import java.math.BigDecimal;

public record DispatchShipperSnapshot(
        Long shipperId, double latitude, double longitude, boolean online, boolean fresh,
        boolean busy, boolean batchCapable, BigDecimal codCapacity,
        long idleSeconds, int recentOfferCount) { }
