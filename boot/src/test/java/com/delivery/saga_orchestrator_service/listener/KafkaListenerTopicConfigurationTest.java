package com.delivery.saga_orchestrator_service.listener;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;

import java.lang.reflect.Method;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaListenerTopicConfigurationTest {

    @Test
    void allInputTopicsKeepCanonicalDefaultsAndUseFactoryGroup() {
        assertListener("handleOrderCreated", "${app.kafka.input-topics.order-created:order.created}");
        assertListener("handleOrderCancelled", "${app.kafka.input-topics.order-cancelled:order.cancelled}");
        assertListener("handleRestaurantConfirmed", "${app.kafka.input-topics.restaurant-confirmed:restaurant.order-confirmed}");
        assertListener("handleDeliveryCreated", "${app.kafka.input-topics.delivery-created:delivery.created.result}");
        assertListener("handleShipperAccepted", "${app.kafka.input-topics.shipper-accepted:delivery.shipper-accepted}");
        assertListener("handleDeliveryStatusUpdated", "${app.kafka.input-topics.delivery-status:delivery.status-updated}");
        assertListener("handleShipperRejected", "${app.kafka.input-topics.shipper-rejected:delivery.shipper-rejected}");
        assertListener("handleShipperFound", "${app.kafka.input-topics.shipper-found:shipper.found}");
        assertListener("handleShipperNotFound", "${app.kafka.input-topics.shipper-not-found:shipper.not-found}");
        assertListener("handleDeliveryCreationFailed", "${app.kafka.input-topics.delivery-created-failed:delivery.created.failed}");
        assertListener("handleDeliveryCancelFailed", "${app.kafka.input-topics.delivery-cancel-failed:delivery.cancel.failed}");
    }

    @Test
    void sharedSourceRetryTopologyIsOwnedBySaga() {
        RetryableTopic retry = KafkaEventListener.class.getAnnotation(RetryableTopic.class);

        assertThat(retry).isNotNull();
        assertThat(retry.retryTopicSuffix()).isEqualTo("-retry-saga");
        assertThat(retry.dltTopicSuffix()).isEqualTo(".saga.DLT");
        assertThat(retry.autoCreateTopics()).isEqualTo("${app.kafka.retry.auto-create-topics:false}");
    }

    private void assertListener(String methodName, String topic) {
        Method method = Arrays.stream(KafkaEventListener.class.getDeclaredMethods())
                .filter(candidate -> candidate.getName().equals(methodName))
                .findFirst()
                .orElseThrow();
        KafkaListener annotation = method.getAnnotation(KafkaListener.class);
        assertThat(annotation).isNotNull();
        assertThat(annotation.topics()).containsExactly(topic);
        assertThat(annotation.groupId()).isEmpty();
    }
}
