package com.delivery.match_service.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "app.resilience.settlement")
public class SettlementCallResilienceProperties {
    private long timeoutMs = 2000;
    private float failureRateThreshold = 50;
    private int slidingWindowSize = 20;
    private long waitOpenSeconds = 30;
    private int permittedHalfOpenCalls = 5;

    public long getTimeoutMs() { return timeoutMs; }
    public void setTimeoutMs(long timeoutMs) { this.timeoutMs = timeoutMs; }
    public float getFailureRateThreshold() { return failureRateThreshold; }
    public void setFailureRateThreshold(float failureRateThreshold) { this.failureRateThreshold = failureRateThreshold; }
    public int getSlidingWindowSize() { return slidingWindowSize; }
    public void setSlidingWindowSize(int slidingWindowSize) { this.slidingWindowSize = slidingWindowSize; }
    public long getWaitOpenSeconds() { return waitOpenSeconds; }
    public void setWaitOpenSeconds(long waitOpenSeconds) { this.waitOpenSeconds = waitOpenSeconds; }
    public int getPermittedHalfOpenCalls() { return permittedHalfOpenCalls; }
    public void setPermittedHalfOpenCalls(int permittedHalfOpenCalls) { this.permittedHalfOpenCalls = permittedHalfOpenCalls; }
}
