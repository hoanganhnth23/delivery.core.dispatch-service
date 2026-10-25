package com.delivery.dispatch.infrastructure.listener;

import com.delivery.dispatch.infrastructure.service.DispatchCoordinator;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class KafkaEventListenerAcknowledgmentTest {

    @Test
    void successfulMutationIsAcknowledged() {
        DispatchCoordinator dispatchCoordinator = mock(DispatchCoordinator.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        String message = "{\"orderId\":7,\"eventId\":\"11111111-1111-1111-1111-111111111111\"}";
        new KafkaEventListener(dispatchCoordinator).handleOrderCreated(message, acknowledgment);

        verify(dispatchCoordinator).handleOrderCreated(7L, message);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void deliveryShipperNotFoundStatusIsAcknowledgedAfterManagerHandlesIt() {
        DispatchCoordinator dispatchCoordinator = mock(DispatchCoordinator.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        String message = "{\"eventId\":\"11111111-1111-1111-1111-111111111111\","
                + "\"orderId\":7,\"deliveryId\":8,\"status\":\"SHIPPER_NOT_FOUND\"}";
        new KafkaEventListener(dispatchCoordinator).handleDeliveryStatusUpdated(message, acknowledgment);

        verify(dispatchCoordinator).handleDeliveryStatusUpdated(7L, 8L, "SHIPPER_NOT_FOUND", message);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void managerFailureIsNotAcknowledged() {
        DispatchCoordinator dispatchCoordinator = mock(DispatchCoordinator.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        doThrow(new RuntimeException("database unavailable"))
                .when(dispatchCoordinator).handleOrderCreated(eq(7L), anyString());

        assertThrows(IllegalStateException.class,
                () -> new KafkaEventListener(dispatchCoordinator)
                        .handleOrderCreated("{\"orderId\":7,\"eventId\":\"11111111-1111-1111-1111-111111111111\"}", acknowledgment));

        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void invalidMessageIsNotAcknowledged() {
        DispatchCoordinator dispatchCoordinator = mock(DispatchCoordinator.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        assertThrows(IllegalArgumentException.class,
                () -> new KafkaEventListener(dispatchCoordinator)
                        .handleOrderCreated("{}", acknowledgment));

        verifyNoInteractions(dispatchCoordinator);
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void nonPositiveIdentityIsSentToRetryAndDlt() {
        DispatchCoordinator dispatchCoordinator = mock(DispatchCoordinator.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        assertThrows(IllegalArgumentException.class,
                () -> new KafkaEventListener(dispatchCoordinator)
                        .handleShipperRejected(
                                "{\"orderId\":7,\"deliveryId\":9,\"rejectedShipperId\":0}",
                                acknowledgment));

        verifyNoInteractions(dispatchCoordinator, acknowledgment);
    }

    @Test
    void cancellationWithoutStableEventIdIsNotAcknowledged() {
        DispatchCoordinator dispatchCoordinator = mock(DispatchCoordinator.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        assertThrows(IllegalArgumentException.class,
                () -> new KafkaEventListener(dispatchCoordinator)
                        .handleOrderCancelled("{\"orderId\":7}", acknowledgment));

        verifyNoInteractions(dispatchCoordinator, acknowledgment);
    }
}
