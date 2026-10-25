package com.delivery.dispatch.infrastructure.service;

/**
 * The outbox could not store a command. It always propagates unchanged so the
 * whole case transaction rolls back and the inbound event is retried.
 */
final class DispatchCommandPublishException extends RuntimeException {

    DispatchCommandPublishException(String message, Throwable cause) {
        super(message, cause);
    }

    /** Re-throws an outbox failure caught by a broad handler that would otherwise wrap it. */
    static void rethrowIfPublishFailure(Exception e) {
        if (e instanceof DispatchCommandPublishException publishException) {
            throw publishException;
        }
    }
}
