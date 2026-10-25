package com.delivery.dispatch.application.api;

/** Durable (outbox) commands the coordinator issues to other services. */
public interface DispatchCommands {

    /** Commands Order to converge to a status, sequenced per case. */
    void orderStatus(DispatchCase dispatchCase, String status, String causeEvent);
}
