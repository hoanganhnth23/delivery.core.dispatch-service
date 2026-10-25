package com.delivery.dispatch.application;

import com.delivery.dispatch.application.DefaultDeliveryProgressUseCaseTest.FakeCase;
import com.delivery.dispatch.application.api.StepFailureUseCase.Outcome;
import com.delivery.dispatch.domain.DispatchStatus;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DefaultStepFailureUseCaseTest {

    private final List<String> calls = new ArrayList<>();
    private final DefaultStepFailureUseCase useCase = new DefaultStepFailureUseCase(
            c -> calls.add("save:" + c.status()),
            (c, command, stop, cause) -> {
                calls.add("compensate:" + command + ":" + stop);
                return "correlated";
            },
            (c, status, cause) -> calls.add("order:" + status + ":" + cause));

    @Test
    void failureBeforeMatchingCancelsDelivery() {
        FakeCase c = new FakeCase(DispatchStatus.DELIVERY_CREATED);
        assertEquals(Outcome.COMPENSATED, useCase.onStepFailed(c, "TIMEOUT_DELIVERY_CREATED", "t"));
        assertEquals(DispatchStatus.FAILED, c.status);
        assertTrue(c.completed);
        assertTrue(c.history().has("TIMEOUT_DELIVERY_CREATED_FAILED"));
        assertEquals(List.of("compensate:CANCEL_DELIVERY:false", "order:CANCELLED:correlated", "save:FAILED"), calls);
    }

    @Test
    void failureDuringMatchingConvergesToShipperNotFoundAndStopsGeneration() {
        assertEquals(Outcome.COMPENSATED, useCase.onStepFailed(new FakeCase(DispatchStatus.FINDING_SHIPPER),
                "TIMEOUT_FINDING_SHIPPER", "t"));
        assertEquals(List.of("compensate:MARK_SHIPPER_NOT_FOUND:true", "order:SHIPPER_NOT_FOUND:correlated",
                "save:FAILED"), calls);
    }

    @Test
    void failureWithNothingToCleanOnlyCancelsOrder() {
        assertEquals(Outcome.COMPENSATED,
                useCase.onStepFailed(new FakeCase(DispatchStatus.STARTED), "TIMEOUT_STARTED", "t"));
        assertEquals(List.of("order:CANCELLED:t", "save:FAILED"), calls);
    }

    @Test
    void deliveryCancelRefusalIsRecordedAndTerminalFailuresIgnored() {
        FakeCase compensating = new FakeCase(DispatchStatus.COMPENSATING);
        assertEquals(Outcome.CANCEL_REFUSAL_RECORDED, useCase.onStepFailed(compensating, "DELIVERY_CANCEL", "r"));
        assertEquals(DispatchStatus.FAILED, compensating.status);
        assertTrue(compensating.history().has("DELIVERY_CANCEL_FAILED"));
        assertEquals(Outcome.IGNORED_TERMINAL,
                useCase.onStepFailed(new FakeCase(DispatchStatus.COMPLETED), "TIMEOUT_STARTED", "t"));
        assertEquals(List.of("save:FAILED"), calls);
    }

    @Test
    void requiresAllCollaborators() {
        assertThrows(NullPointerException.class,
                () -> new DefaultStepFailureUseCase(null, (c, d, s, e) -> e, (c, s, e) -> { }));
        assertThrows(NullPointerException.class,
                () -> new DefaultStepFailureUseCase(c -> { }, null, (c, s, e) -> { }));
        assertThrows(NullPointerException.class,
                () -> new DefaultStepFailureUseCase(c -> { }, (c, d, s, e) -> e, null));
    }
}
