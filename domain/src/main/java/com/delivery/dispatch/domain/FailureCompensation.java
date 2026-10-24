package com.delivery.dispatch.domain;

/**
 * Compensation chosen from the status observed before a step failed.
 *
 * @param deliveryCommand command to Delivery, or {@link DeliveryCommand#NONE}
 * @param stopMatching    whether the current matching generation must be stopped
 * @param orderStatus     terminal status commanded to Order
 */
public record FailureCompensation(DeliveryCommand deliveryCommand, boolean stopMatching, String orderStatus) {

    public enum DeliveryCommand { NONE, CANCEL_DELIVERY, MARK_SHIPPER_NOT_FOUND }

    /** Step name of a Delivery refusal to cancel. */
    public static final String DELIVERY_CANCEL_STEP = "DELIVERY_CANCEL";

    public enum Outcome {
        /** Delivery refused cancellation after compensation began; record FAILED for manual reconciliation. */
        RECORD_CANCEL_REFUSAL,
        /** Case already terminal; ignore. */
        IGNORE_TERMINAL,
        /** Apply {@link #forPreviousStatus(DispatchStatus)} and finish FAILED. */
        COMPENSATE
    }

    public static Outcome outcome(String stepName, DispatchStatus status) {
        if (DELIVERY_CANCEL_STEP.equals(stepName)
                && (status == DispatchStatus.COMPENSATING
                        || status == DispatchStatus.CANCELLED
                        || status == DispatchStatus.FAILED)) {
            return Outcome.RECORD_CANCEL_REFUSAL;
        }
        if (status.isTerminal()) {
            return Outcome.IGNORE_TERMINAL;
        }
        return Outcome.COMPENSATE;
    }

    /**
     * A delivery that never entered matching is cancelled. Once matching
     * started, the canonical terminal outcome is SHIPPER_NOT_FOUND.
     */
    public static FailureCompensation forPreviousStatus(DispatchStatus previous) {
        return switch (previous) {
            case DELIVERY_CREATED -> new FailureCompensation(DeliveryCommand.CANCEL_DELIVERY, false, "CANCELLED");
            case FINDING_SHIPPER, SHIPPER_FOUND ->
                    new FailureCompensation(DeliveryCommand.MARK_SHIPPER_NOT_FOUND, true, "SHIPPER_NOT_FOUND");
            default -> new FailureCompensation(DeliveryCommand.NONE, false, "CANCELLED");
        };
    }
}
