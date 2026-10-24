package com.delivery.dispatch.application.api;

/** Persists a case mutated inside the caller's transaction. */
public interface DispatchCaseStore {

    void save(DispatchCase dispatchCase);
}
