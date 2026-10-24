package com.delivery.saga_orchestrator_service.listener;

import com.delivery.saga_orchestrator_service.service.SagaManager;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class KafkaEventListenerAcknowledgmentTest {

    @Test
    void successfulMutationIsAcknowledged() {
        SagaManager sagaManager = mock(SagaManager.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        String message = "{\"orderId\":7,\"eventId\":\"11111111-1111-1111-1111-111111111111\"}";
        new KafkaEventListener(sagaManager).handleOrderCreated(message, acknowledgment);

        verify(sagaManager).handleOrderCreated(7L, message);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void deliveryShipperNotFoundStatusIsAcknowledgedAfterManagerHandlesIt() {
        SagaManager sagaManager = mock(SagaManager.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        String message = "{\"eventId\":\"11111111-1111-1111-1111-111111111111\","
                + "\"orderId\":7,\"deliveryId\":8,\"status\":\"SHIPPER_NOT_FOUND\"}";
        new KafkaEventListener(sagaManager).handleDeliveryStatusUpdated(message, acknowledgment);

        verify(sagaManager).handleDeliveryStatusUpdated(7L, 8L, "SHIPPER_NOT_FOUND", message);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void managerFailureIsNotAcknowledged() {
        SagaManager sagaManager = mock(SagaManager.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        doThrow(new RuntimeException("database unavailable"))
                .when(sagaManager).handleOrderCreated(eq(7L), anyString());

        assertThrows(IllegalStateException.class,
                () -> new KafkaEventListener(sagaManager)
                        .handleOrderCreated("{\"orderId\":7,\"eventId\":\"11111111-1111-1111-1111-111111111111\"}", acknowledgment));

        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void invalidMessageIsNotAcknowledged() {
        SagaManager sagaManager = mock(SagaManager.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        assertThrows(IllegalArgumentException.class,
                () -> new KafkaEventListener(sagaManager)
                        .handleOrderCreated("{}", acknowledgment));

        verifyNoInteractions(sagaManager);
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void nonPositiveIdentityIsSentToRetryAndDlt() {
        SagaManager sagaManager = mock(SagaManager.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        assertThrows(IllegalArgumentException.class,
                () -> new KafkaEventListener(sagaManager)
                        .handleShipperRejected(
                                "{\"orderId\":7,\"deliveryId\":9,\"rejectedShipperId\":0}",
                                acknowledgment));

        verifyNoInteractions(sagaManager, acknowledgment);
    }

    @Test
    void cancellationWithoutStableEventIdIsNotAcknowledged() {
        SagaManager sagaManager = mock(SagaManager.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        assertThrows(IllegalArgumentException.class,
                () -> new KafkaEventListener(sagaManager)
                        .handleOrderCancelled("{\"orderId\":7}", acknowledgment));

        verifyNoInteractions(sagaManager, acknowledgment);
    }
}
