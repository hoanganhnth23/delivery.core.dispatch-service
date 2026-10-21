package com.delivery.match.domain.single;

/** Command overrides are preserved verbatim, including values rejected later by the reactive adapter. */
public record RetryPolicy(int maxRetries, int initialDelaySeconds, int maxDelaySeconds, double multiplier) {
    public static RetryPolicy from(Integer retries, Integer initialDelay, Integer maxDelay, Double multiplier) {
        return new RetryPolicy(retries == null ? 10 : retries, initialDelay == null ? 30 : initialDelay,
                maxDelay == null ? 300 : maxDelay, multiplier == null ? 1.5 : multiplier);
    }

    public long delayMs(long totalRetries) {
        return totalRetries == 0 ? initialDelaySeconds * 1000L
                : Math.min((long) (initialDelaySeconds * Math.pow(multiplier, totalRetries) * 1000),
                        maxDelaySeconds * 1000L);
    }
}
