package com.delivery.match_service.listener;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaListenerTopicConfigurationTest {

    @Test
    void findCommandUsesTheProvisionedDotRetryTopology() throws Exception {
        Method method = FindShipperEventListener.class.getDeclaredMethod(
                "handleFindShipperEvent", String.class, String.class, Integer.class, Long.class);

        KafkaListener listener = method.getAnnotation(KafkaListener.class);
        RetryableTopic retry = method.getAnnotation(RetryableTopic.class);

        assertThat(listener).isNotNull();
        assertThat(listener.topics()).containsExactly(
                "${app.kafka.topics.find-shipper:saga.command.find-shipper}");
        assertThat(listener.autoStartup()).isEqualTo(
                "${match.kafka.find-listener.auto-startup:${match.kafka.listener.auto-startup:true}}");
        assertThat(retry).isNotNull();
        assertThat(retry.attempts()).isEqualTo("4");
        assertThat(retry.retryTopicSuffix()).isEqualTo(".retry");
        assertThat(retry.dltTopicSuffix()).isEqualTo(".DLT");
        assertThat(retry.autoCreateTopics()).isEqualTo("false");
    }

    @Test
    void stopCommandUsesTheCanonicalSourceForTheDefaultDltHandler() throws Exception {
        Method method = FindShipperEventListener.class.getDeclaredMethod(
                "handleStopMatchingCommand", String.class,
                org.springframework.kafka.support.Acknowledgment.class);

        KafkaListener listener = method.getAnnotation(KafkaListener.class);

        assertThat(listener).isNotNull();
        assertThat(listener.topics()).containsExactly("saga.command.stop-matching");
        assertThat(listener.autoStartup()).isEqualTo(
                "${match.kafka.stop-listener.auto-startup:${match.kafka.listener.auto-startup:true}}");
        assertThat(method.getAnnotation(RetryableTopic.class)).isNull();
    }
}
