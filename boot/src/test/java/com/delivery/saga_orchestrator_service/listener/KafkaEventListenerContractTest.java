package com.delivery.saga_orchestrator_service.listener;

import com.delivery.saga_orchestrator_service.service.SagaManager;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class KafkaEventListenerContractTest {
    private static final String MESSAGE = "{\"eventId\":\"11111111-1111-1111-1111-111111111111\","
            + "\"orderId\":7,\"deliveryId\":8,\"shipperId\":9,\"rejectedShipperId\":10,"
            + "\"newStatus\":\"DELIVERED\",\"status\":\"PICKED_UP\",\"reason\":\"failure reason\"}";
    enum Event { ORDER_CREATED, ORDER_CANCELLED, RESTAURANT_CONFIRMED, DELIVERY_CREATED, SHIPPER_ACCEPTED,
        OFFER_PERSISTED, OFFER_RETIRED, STATUS_UPDATED, SHIPPER_REJECTED, SHIPPER_FOUND, SHIPPER_NOT_FOUND,
        CREATION_FAILED, CANCEL_FAILED }

    @ParameterizedTest
    @EnumSource(Event.class)
    void eachEventMapsIdentitiesAndPayloadAndAcknowledgesAfterManagerReturns(Event event) {
        SagaManager manager = mock(SagaManager.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        invoke(event, new KafkaEventListener(manager), MESSAGE, ack);
        var ordered = inOrder(manager, ack);
        verifyDelegation(event, ordered.verify(manager), MESSAGE);
        ordered.verify(ack).acknowledge();
        verifyNoMoreInteractions(manager, ack);
    }

    @ParameterizedTest
    @EnumSource(Event.class)
    void eachEventLeavesInfrastructureFailureUnacknowledgedForRetry(Event event) {
        RuntimeException databaseFailure = new RuntimeException("database unavailable");
        SagaManager manager = mock(SagaManager.class, call -> { throw databaseFailure; });
        Acknowledgment ack = mock(Acknowledgment.class);
        assertThatThrownBy(() -> invoke(event, new KafkaEventListener(manager), MESSAGE, ack))
                .isInstanceOf(IllegalStateException.class).hasCause(databaseFailure)
                .hasMessageStartingWith("Failed to process ");
        verifyNoInteractions(ack);
    }

    @ParameterizedTest
    @EnumSource(Event.class)
    void eachEventRejectsMissingEnvelopeBeforeManagerOrAck(Event event) {
        SagaManager manager = mock(SagaManager.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        assertThatThrownBy(() -> invoke(event, new KafkaEventListener(manager), "{}", ack))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(manager, ack);
    }

    @ParameterizedTest
    @ValueSource(strings = {"null", "7", "\"invalid\""})
    void malformedEventIdentityIsPoisonAndNeverAcknowledged(String eventId) {
        SagaManager manager = mock(SagaManager.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        assertThatThrownBy(() -> new KafkaEventListener(manager).handleOrderCancelled(
                "{\"orderId\":7,\"eventId\":" + eventId + "}", ack))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("eventId is required and must be a UUID");
        verifyNoInteractions(manager, ack);
    }

    @ParameterizedTest
    @ValueSource(strings = {"null", "\"7\"", "0", "-1"})
    void invalidOrderIdentityIsPoisonAndNeverAcknowledged(String orderId) {
        SagaManager manager = mock(SagaManager.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        assertThatThrownBy(() -> new KafkaEventListener(manager).handleOrderCreated(
                MESSAGE.replace("\"orderId\":7", "\"orderId\":" + orderId), ack))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("orderId ");
        verifyNoInteractions(manager, ack);
    }

    @Test
    void missingStatusIsRejectedAndMissingFailureReasonUsesExistingDefault() {
        SagaManager manager = mock(SagaManager.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        KafkaEventListener listener = new KafkaEventListener(manager);
        String minimal = "{\"eventId\":\"11111111-1111-1111-1111-111111111111\",\"orderId\":7,\"deliveryId\":8}";
        assertThatThrownBy(() -> listener.handleDeliveryStatusUpdated(minimal, ack))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("delivery status is required");
        verifyNoInteractions(manager, ack);
        listener.handleDeliveryCreationFailed(minimal, ack);
        verify(manager).handleDeliveryCreationFailed(7L, "Unknown error", minimal);
        verify(ack).acknowledge();
    }

    private void invoke(Event event, KafkaEventListener listener, String message, Acknowledgment ack) {
        switch (event) {
            case ORDER_CREATED -> listener.handleOrderCreated(message, ack);
            case ORDER_CANCELLED -> listener.handleOrderCancelled(message, ack);
            case RESTAURANT_CONFIRMED -> listener.handleRestaurantConfirmed(message, ack);
            case DELIVERY_CREATED -> listener.handleDeliveryCreated(message, ack);
            case SHIPPER_ACCEPTED -> listener.handleShipperAccepted(message, ack);
            case OFFER_PERSISTED -> listener.handleOfferPersisted(message, ack);
            case OFFER_RETIRED -> listener.handleOfferRetired(message, ack);
            case STATUS_UPDATED -> listener.handleDeliveryStatusUpdated(message, ack);
            case SHIPPER_REJECTED -> listener.handleShipperRejected(message, ack);
            case SHIPPER_FOUND -> listener.handleShipperFound(message, ack);
            case SHIPPER_NOT_FOUND -> listener.handleShipperNotFound(message, ack);
            case CREATION_FAILED -> listener.handleDeliveryCreationFailed(message, ack);
            case CANCEL_FAILED -> listener.handleDeliveryCancelFailed(message, ack);
        }
    }

    private void verifyDelegation(Event event, SagaManager verified, String message) {
        switch (event) {
            case ORDER_CREATED -> verified.handleOrderCreated(7L, message);
            case ORDER_CANCELLED -> verified.handleOrderCancelled(7L, message);
            case RESTAURANT_CONFIRMED -> verified.handleRestaurantConfirmed(7L, message);
            case DELIVERY_CREATED -> verified.handleDeliveryCreated(7L, 8L, message);
            case SHIPPER_ACCEPTED -> verified.handleShipperAccepted(7L, 8L, 9L, message);
            case OFFER_PERSISTED -> verified.handleOfferPersisted(7L, 8L, message);
            case OFFER_RETIRED -> verified.handleOfferRetired(7L, 8L, message);
            case STATUS_UPDATED -> verified.handleDeliveryStatusUpdated(7L, 8L, "DELIVERED", message);
            case SHIPPER_REJECTED -> verified.handleShipperRejected(7L, 8L, 10L, message);
            case SHIPPER_FOUND -> verified.handleShipperFound(7L, 8L, message);
            case SHIPPER_NOT_FOUND -> verified.handleShipperNotFound(7L, 8L, message);
            case CREATION_FAILED -> verified.handleDeliveryCreationFailed(7L, "failure reason", message);
            case CANCEL_FAILED -> verified.handleStepFailed("DELIVERY_CANCEL", 7L, "failure reason", message);
        }
    }
}
