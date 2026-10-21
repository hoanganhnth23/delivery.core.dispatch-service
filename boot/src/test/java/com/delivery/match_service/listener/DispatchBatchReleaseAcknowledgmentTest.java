package com.delivery.match_service.listener;

import com.delivery.match_service.MatchServiceApplication;
import com.delivery.match_service.entity.DispatchPoolItem;
import com.delivery.match_service.repository.DispatchPoolItemRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/** The Kafka offset of a batch release may be acknowledged only after the pool retirement commits. */
@SpringBootTest(classes = MatchServiceApplication.class, properties = {
        "spring.kafka.listener.auto-startup=false",
        "match.kafka.listener.auto-startup=false",
        "app.outbox.relay-enabled=false",
        "matching.batch.enabled=true",
        "matching.batch.scheduler-enabled=false"
})
class DispatchBatchReleaseAcknowledgmentTest {

    private static final long DELIVERY_ID = 880001L;
    private static final UUID SESSION = UUID.fromString("88888888-0000-0000-0000-000000000001");

    @Autowired DispatchBatchReleaseListener listener;
    @Autowired DispatchPoolItemRepository poolRepository;
    @Autowired PlatformTransactionManager transactionManager;

    @BeforeEach
    void seed() {
        poolRepository.deleteAll();
        DispatchPoolItem item = new DispatchPoolItem();
        LocalDateTime now = LocalDateTime.now();
        item.setPoolItemId(UUID.randomUUID());
        item.setOrderId(990001L);
        item.setDeliveryId(DELIVERY_ID);
        item.setMatchingSessionId(SESSION);
        item.setPickupH3Cell("8865b6b6c9fffff");
        item.setPickupLat(10.77);
        item.setPickupLng(106.70);
        item.setDeliveryLat(10.78);
        item.setDeliveryLng(106.71);
        item.setTotalPrice(new BigDecimal("100000"));
        item.setPaymentMethod("COD");
        item.setWaveNumber(1);
        item.setEligibleAt(now);
        item.setMatchingDeadlineAt(now.plusMinutes(5));
        item.setState(DispatchPoolItem.State.CLAIMED);
        item.setCreatedAt(now);
        item.setUpdatedAt(now);
        poolRepository.saveAndFlush(item);
    }

    private static String release() {
        return "{\"deliveryIds\":[" + DELIVERY_ID + "],\"matchingSessionIds\":[\"" + SESSION + "\"]}";
    }

    private DispatchPoolItem.State state() {
        return poolRepository.findAll().get(0).getState();
    }

    @Test
    void enclosingRollbackLeavesReleaseUnacknowledgedAndPoolUnchanged() {
        Acknowledgment ack = mock(Acknowledgment.class);
        new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
            listener.handle(release(), ack);
            status.setRollbackOnly();
        });
        verify(ack, never()).acknowledge();
        assertThat(state()).isEqualTo(DispatchPoolItem.State.CLAIMED);
    }

    @Test
    void enclosingCommitAcknowledgesOnlyAfterRetirementCommits() {
        Acknowledgment ack = mock(Acknowledgment.class);
        new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
            listener.handle(release(), ack);
            verify(ack, never()).acknowledge();
        });
        verify(ack).acknowledge();
        assertThat(state()).isEqualTo(DispatchPoolItem.State.EXPIRED);
    }

    @Test
    void directDeliveryAcknowledgesOnceAfterItsOwnCommit() {
        Acknowledgment ack = mock(Acknowledgment.class);
        listener.handle(release(), ack);
        verify(ack, times(1)).acknowledge();
        assertThat(state()).isEqualTo(DispatchPoolItem.State.EXPIRED);
    }
}
