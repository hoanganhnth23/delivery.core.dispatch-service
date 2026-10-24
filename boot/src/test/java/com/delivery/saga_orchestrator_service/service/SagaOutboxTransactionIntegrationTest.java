package com.delivery.saga_orchestrator_service.service;

import com.delivery.saga_orchestrator_service.repository.SagaInstanceRepository;
import com.delivery.saga_orchestrator_service.repository.SagaOutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.support.TransactionTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:saga_outbox;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
        "spring.kafka.listener.auto-startup=false",
        "spring.flyway.enabled=false",
        "app.outbox.relay-enabled=false"
})
class SagaOutboxTransactionIntegrationTest {

    @Autowired SagaManager sagaManager;
    @Autowired SagaInstanceRepository sagaRepository;
    @Autowired SagaOutboxEventRepository outboxRepository;
    @Autowired TransactionTemplate transactionTemplate;
    @Autowired ObjectMapper objectMapper;

    @BeforeEach
    void clean() {
        outboxRepository.deleteAll();
        sagaRepository.deleteAll();
    }

    @Test
    void sagaTransitionAndCommandCommitAtomically() throws Exception {
        sagaManager.handleOrderCreated(101L, orderCreated(101L));

        assertThat(sagaRepository.count()).isEqualTo(1);
        assertThat(outboxRepository.count()).isEqualTo(1);
        var command = outboxRepository.findAll().get(0);
        var payload = objectMapper.readTree(command.getPayload());
        assertThat(command.getAggregateId()).isEqualTo("101");
        assertThat(command.getTopic()).isEqualTo(SagaManager.CMD_CREATE_DELIVERY);
        assertThat(payload.path("eventId").asText()).isEqualTo(command.getEventId().toString());
        assertThat(payload.path("orderId").asLong()).isEqualTo(101L);
    }

    @Test
    void rollbackRemovesBothSagaTransitionAndCommand() {
        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            sagaManager.handleOrderCreated(102L, orderCreated(102L));
            throw new DeliberateRollback();
        })).isInstanceOf(DeliberateRollback.class);

        assertThat(sagaRepository.count()).isZero();
        assertThat(outboxRepository.count()).isZero();
    }

    private String orderCreated(Long orderId) {
        return """
                {"eventId":"00000000-0000-0000-0000-%012d","orderId":%d,
                 "totalPrice":120000,"shippingFee":20000,
                 "paymentMethod":"COD","restaurantId":30}
                """.formatted(orderId, orderId);
    }

    private static final class DeliberateRollback extends RuntimeException {
    }
}
