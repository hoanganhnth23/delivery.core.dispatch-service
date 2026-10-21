package com.delivery.match_service.exception;

/**
 * ✅ Custom Exception cho Match Service
 * Theo Backend Instructions: Custom exceptions với meaningful names
 */
public class MatchServiceException extends RuntimeException {
    
    public MatchServiceException(String message) {
        super(message);
    }
    
    public MatchServiceException(String message, Throwable cause) {
        super(message, cause);
    }
}
