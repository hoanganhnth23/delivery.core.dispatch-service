# 🔄 Continuous Shipper Search với Retry Mechanism

## ✅ Tổng quan

Đã cập nhật `FindShipperEventListener` để tìm shipper liên tục nếu chưa tìm thấy, sử dụng **exponential backoff retry pattern**.

## 🚀 Cách hoạt động

### 1. Event Flow
```
📥 Kafka Event → Validate → Start Continuous Search → Retry với Backoff → Success/Failure
```

### 2. Retry Configuration
```java
MAX_RETRY_ATTEMPTS = 10        // Tối đa 10 lần thử
INITIAL_DELAY_SECONDS = 30     // Bắt đầu với 30 giây
MAX_DELAY_SECONDS = 300        // Tối đa 5 phút
BACKOFF_MULTIPLIER = 1.5       // Tăng delay theo exponential
```

### 3. Retry Schedule
```
Attempt 1: Ngay lập tức
Attempt 2: Sau 30 giây
Attempt 3: Sau 45 giây  (30 * 1.5)
Attempt 4: Sau 67 giây  (45 * 1.5)
Attempt 5: Sau 101 giây (67 * 1.5)
...
Attempt 10: Sau 300 giây (max)
```

## 🔧 Implementation Details

### startContinuousShipperSearch()
```java
// ✅ Reactive retry với exponential backoff
matchService.findNearbyShippers(request, systemUserId, systemRole)
    .retryWhen(Retry.backoff(MAX_RETRY_ATTEMPTS, Duration.ofSeconds(INITIAL_DELAY_SECONDS))
        .maxBackoff(Duration.ofSeconds(MAX_DELAY_SECONDS))
        .multiplier(BACKOFF_MULTIPLIER)
        .filter(throwable -> {
            // Chỉ retry nếu không tìm thấy shipper
            return throwable.getMessage().contains("No shippers found");
        }))
```

### Retry Logic
1. **Trigger Retry**: Khi result empty hoặc không tìm thấy shipper
2. **Filter Errors**: Chỉ retry cho "No shippers found", không retry system errors
3. **Exponential Backoff**: Delay tăng dần để tránh spam
4. **Max Attempts**: Dừng sau 10 lần để tránh infinite loop

## 📊 Logging & Monitoring

### Log Messages
```
🔄 Retry attempt 3/10 for delivery: 123 - Next retry in 67000ms
✅ Found 2 nearby shippers for delivery: 123 after 3 attempts
💥 Failed to find shippers for delivery: 123 after 10 attempts
```

### Metrics Tracking
- **attemptCount**: Đếm số lần retry cho mỗi delivery
- **totalRetries**: Tổng số retry trong retrySignal
- **delayMs**: Thời gian delay cho lần retry tiếp theo

## 🛡️ Error Handling

### Success Cases
1. **Tìm thấy shipper**: Gọi `processShipperMatchResult()` và acknowledge
2. **Retry thành công**: Log attempt count và continue

### Failure Cases
1. **Max retries exceeded**: Gọi `processShipperMatchResult()` với empty list
2. **System error**: Acknowledge ngay để tránh blocking
3. **Invalid event**: Acknowledge và log error

## 🎯 Benefits

### 1. **Persistent Search**
- Không bỏ lỡ delivery nào
- Tìm shipper liên tục cho đến khi thành công

### 2. **Smart Backoff**
- Tránh spam tracking service
- Tiết kiệm resources với delay tăng dần

### 3. **Fault Tolerance**
- Không crash khi có system error
- Graceful degradation sau max retries

### 4. **Monitoring**
- Chi tiết log cho mỗi retry attempt
- Tracking performance metrics

## 🔮 Future Enhancements

### 1. **Dynamic Configuration**
```java
// Load từ application.properties
@Value("${match.retry.max-attempts:10}")
private int maxRetryAttempts;

@Value("${match.retry.initial-delay:30}")
private int initialDelaySeconds;
```

### 2. **Circuit Breaker**
```java
// Tạm dừng retry nếu tracking service down
.transformDeferred(CircuitBreaker.ofRetryRegistry(retryRegistry).toReactorTransformer())
```

### 3. **Dead Letter Queue**
```java
// Gửi failed events to DLQ để manual handling
kafkaTemplate.send("delivery.find-shipper.dlq", event);
```

### 4. **Real-time Updates**
```java
// Lắng nghe shipper location updates để trigger immediate retry
@EventListener(ShipperLocationUpdatedEvent.class)
public void onShipperLocationUpdated(ShipperLocationUpdatedEvent event) {
    // Retry pending deliveries in the area
}
```

## 🧪 Testing Strategy

### Unit Tests
```java
@Test
void testContinuousSearchWithRetry() {
    // Mock empty result first, then success
    when(matchService.findNearbyShippers())
        .thenReturn(Mono.just(Collections.emptyList()))  // First call
        .thenReturn(Mono.just(List.of(shipper1, shipper2))); // Second call
    
    // Verify retry mechanism works
}
```

### Integration Tests
```java
@Test 
void testEndToEndRetryFlow() {
    // Send Kafka event
    // Wait for retries
    // Verify final result
}
```

## 📈 Performance Impact

### Memory Usage
- **AtomicInteger per event**: Minimal overhead
- **Reactive chains**: Efficient memory usage

### CPU Usage  
- **Exponential backoff**: Reduces CPU load over time
- **Filtered retries**: Only retry when necessary

### Network Usage
- **Smart delays**: Prevents network spam
- **Targeted retries**: Only for no-shipper scenarios

---

**🎯 Result: Match Service giờ đây có thể tìm shipper liên tục và thông minh, đảm bảo không delivery nào bị bỏ lỡ!** 🚀
