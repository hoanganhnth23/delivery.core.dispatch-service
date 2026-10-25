package com.delivery.dispatch.application.api;

/** Applies a Delivery-owned status fact to a locked case. */
public interface DeliveryProgressUseCase {

    enum Outcome { REPLAY, SHIPPER_NOT_FOUND_ECHO_RECORDED, CANCELLATION_CONFIRMED, APPLIED }

    Outcome apply(DispatchCase dispatchCase, String deliveryStatus, String rawEvent);
}
