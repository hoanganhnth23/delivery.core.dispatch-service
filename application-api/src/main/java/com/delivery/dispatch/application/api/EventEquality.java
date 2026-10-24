package com.delivery.dispatch.application.api;

/** Semantic equality of two serialized events (exact replay detection). */
@FunctionalInterface
public interface EventEquality {

    boolean same(String left, String right);
}
