package com.delivery.match_service.listener;

import com.delivery.match_service.service.CompletedDeliveryProjection;
import com.delivery.identity.contracts.SimulationContext;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class DeliveryCompletedProjectionListenerTest {

    @Test
    void recordsSimulationCompletionInItsRunNamespace() {
        CompletedDeliveryProjection projection = mock(CompletedDeliveryProjection.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        DeliveryCompletedProjectionListener listener = new DeliveryCompletedProjectionListener(projection);
        UUID eventId = UUID.randomUUID();
        UUID runId = UUID.randomUUID();
        UUID cohortId = UUID.randomUUID();

        listener.handleDeliveryCompleted("""
                {"eventId":"%s","shipperId":42,"simulationContext":{
                  "mode":"SIMULATION","runId":"%s","cohortId":"%s","bindingVersion":1
                }}
                """.formatted(eventId, runId, cohortId), acknowledgment);

        verify(projection).record(eq(eventId), eq(42L), eq(new SimulationContext(
                SimulationContext.ExecutionMode.SIMULATION, runId, cohortId, 1L)));
        verify(acknowledgment).acknowledge();
    }
}
