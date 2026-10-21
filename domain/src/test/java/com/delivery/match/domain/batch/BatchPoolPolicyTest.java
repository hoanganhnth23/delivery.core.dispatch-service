package com.delivery.match.domain.batch;

import com.delivery.match.domain.batch.BatchPoolPolicy.Admission;
import com.delivery.match.domain.batch.BatchPoolPolicy.State;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class BatchPoolPolicyTest {

    private static final LocalDateTime NOW = LocalDateTime.of(2026, 10, 5, 5, 0);

    @Test
    void intakeRequiresIdentitiesBeforeTheCapabilityFlag() {
        UUID session = UUID.randomUUID();
        BatchPoolPolicy.requireIntake(1L, 2L, session, true);
        String identities = "Valid order, delivery and matching session are required";
        assertEquals(identities, assertThrows(IllegalArgumentException.class,
                () -> BatchPoolPolicy.requireIntake(1L, null, session, false)).getMessage());
        assertThrows(IllegalArgumentException.class, () -> BatchPoolPolicy.requireIntake(1L, 0L, session, true));
        assertThrows(IllegalArgumentException.class, () -> BatchPoolPolicy.requireIntake(null, 2L, session, true));
        assertThrows(IllegalArgumentException.class, () -> BatchPoolPolicy.requireIntake(0L, 2L, session, true));
        assertThrows(IllegalArgumentException.class, () -> BatchPoolPolicy.requireIntake(1L, 2L, null, true));
        assertEquals("Rolling batch dispatch is disabled", assertThrows(IllegalStateException.class,
                () -> BatchPoolPolicy.requireIntake(1L, 2L, session, false)).getMessage());
    }

    @Test
    void deadlineAndWaveDefaults() {
        assertEquals(NOW.plusMinutes(5), BatchPoolPolicy.deadline(NOW, null));
        assertEquals(NOW.plusMinutes(1), BatchPoolPolicy.deadline(NOW, NOW.plusMinutes(1)));
        assertEquals(0, BatchPoolPolicy.initialWave(null));
        assertEquals(0, BatchPoolPolicy.initialWave(-2));
        assertEquals(2, BatchPoolPolicy.initialWave(2));
    }

    @Test
    void roundAdmissionFencesCancellationBeforeDeadline() {
        assertEquals(Admission.CANCEL, BatchPoolPolicy.admission(true, NOW.minusSeconds(1), NOW));
        assertEquals(Admission.RETURN_FOR_EXPIRY, BatchPoolPolicy.admission(false, NOW, NOW));
        assertEquals(Admission.ADMIT, BatchPoolPolicy.admission(false, NOW.plusSeconds(1), NOW));
    }

    @Test
    void requeueRespectsWaveBudget() {
        assertEquals(State.WAITING, BatchPoolPolicy.requeueState(0, 3));
        assertEquals(State.EXPIRED, BatchPoolPolicy.requeueState(3, 3));
        assertEquals(State.EXPIRED, BatchPoolPolicy.requeueState(1, 0));
        assertEquals(State.WAITING, BatchPoolPolicy.requeueState(0, 0));
    }

    @Test
    void expirySweepTouchesOnlyDueWaitingItems() {
        assertTrue(BatchPoolPolicy.expires(State.WAITING, NOW, NOW));
        assertFalse(BatchPoolPolicy.expires(State.WAITING, NOW.plusSeconds(1), NOW));
        assertFalse(BatchPoolPolicy.expires(State.WAITING, null, NOW));
        assertFalse(BatchPoolPolicy.expires(State.CLAIMED, NOW.minusMinutes(1), NOW));
    }

    @Test
    void stopAndBatchReleaseRetireOnlyNonFinalItems() {
        assertTrue(BatchPoolPolicy.retiredByStop(State.WAITING));
        assertTrue(BatchPoolPolicy.retiredByStop(State.CLAIMED));
        assertFalse(BatchPoolPolicy.retiredByStop(State.ASSIGNED));
        assertFalse(BatchPoolPolicy.retiredByStop(State.EXPIRED));
        assertTrue(BatchPoolPolicy.retiredByBatchRelease(State.ASSIGNED));
        assertTrue(BatchPoolPolicy.retiredByBatchRelease(State.CLAIMED));
        assertFalse(BatchPoolPolicy.retiredByBatchRelease(State.CANCELLED));
        assertFalse(BatchPoolPolicy.retiredByBatchRelease(State.EXPIRED));
    }
}
