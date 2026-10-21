package com.delivery.match_service.service;

import java.util.UUID;

/**
 * Lưu/đọc cờ cancel matching theo deliveryId và matching generation trong Redis.
 */
public interface MatchCancellationService {

    /**
     * Đánh dấu một matching generation đã bị cancel (stop matching).
     */
    void markCancelled(Long deliveryId, UUID matchingSessionId);

    /**
     * Kiểm tra matching generation có bị cancel không.
     */
    boolean isCancelled(Long deliveryId, UUID matchingSessionId);
}
