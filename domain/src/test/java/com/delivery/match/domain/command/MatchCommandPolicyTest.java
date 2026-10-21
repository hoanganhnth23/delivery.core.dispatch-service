package com.delivery.match.domain.command;

import com.delivery.match.domain.command.MatchCommandPolicy.CommandIdentity;
import com.delivery.match.domain.command.MatchCommandPolicy.Next;
import com.delivery.match.domain.command.MatchCommandPolicy.OutboxStatus;
import com.delivery.match.domain.command.MatchCommandPolicy.Status;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class MatchCommandPolicyTest {

    private static final UUID SESSION = UUID.fromString("33333333-3333-3333-3333-333333333333");

    @Test
    void exactReplayResumesIncompleteWorkAndNeverRerunsTerminalCommands() {
        assertEquals(Next.PROCESS, MatchCommandPolicy.next(Status.PENDING));
        assertEquals(Next.RESUME, MatchCommandPolicy.next(Status.CANDIDATE_STAGED));
        assertEquals(Next.TERMINAL, MatchCommandPolicy.next(Status.RESULT_STAGED));
        assertEquals(Next.TERMINAL, MatchCommandPolicy.next(Status.CANCELLED));
    }

    @Test
    void replayMustMatchEverySourceIdentityField() {
        CommandIdentity original = new CommandIdentity("t", 1L, 2L, SESSION, "f");
        MatchCommandPolicy.requireExactReplay(original, new CommandIdentity("t", 1L, 2L, SESSION, "f"));
        for (CommandIdentity changed : new CommandIdentity[] {
                new CommandIdentity("u", 1L, 2L, SESSION, "f"),
                new CommandIdentity("t", 9L, 2L, SESSION, "f"),
                new CommandIdentity("t", 1L, 9L, SESSION, "f"),
                new CommandIdentity("t", 1L, 2L, UUID.randomUUID(), "f"),
                new CommandIdentity("t", 1L, 2L, SESSION, "g")}) {
            assertEquals("Match command eventId replay has a contradictory payload",
                    assertThrows(IllegalArgumentException.class,
                            () -> MatchCommandPolicy.requireExactReplay(original, changed)).getMessage());
        }
    }

    @Test
    void generationBelongsToOneCommand() {
        MatchCommandPolicy.requireUnownedGeneration(false);
        assertThrows(IllegalArgumentException.class, () -> MatchCommandPolicy.requireUnownedGeneration(true));
    }

    @Test
    void tombstoneAndStopReplayMustMatchIdentity() {
        MatchCommandPolicy.requireTombstoneIdentity(1L, 2L, SESSION, 1L, 2L, SESSION);
        assertThrows(IllegalArgumentException.class,
                () -> MatchCommandPolicy.requireTombstoneIdentity(1L, 2L, SESSION, 9L, 2L, SESSION));
        assertThrows(IllegalArgumentException.class,
                () -> MatchCommandPolicy.requireTombstoneIdentity(1L, 2L, SESSION, 1L, 9L, SESSION));
        assertThrows(IllegalArgumentException.class,
                () -> MatchCommandPolicy.requireTombstoneIdentity(1L, 2L, SESSION, 1L, 2L, UUID.randomUUID()));
        MatchCommandPolicy.requireExactStopReplay("f", "f");
        assertEquals("stop-matching eventId replay has a contradictory payload",
                assertThrows(IllegalArgumentException.class,
                        () -> MatchCommandPolicy.requireExactStopReplay("f", "g")).getMessage());
    }

    @Test
    void stopCommandRequiresPositiveIdentitiesAndPayload() {
        UUID id = UUID.randomUUID();
        MatchCommandPolicy.requireStopCommand(id, 1L, 2L, SESSION, "{}");
        String identities = "stop-matching eventId, orderId, deliveryId and matchingSessionId are required";
        assertEquals(identities, assertThrows(IllegalArgumentException.class,
                () -> MatchCommandPolicy.requireStopCommand(null, 1L, 2L, SESSION, "{}")).getMessage());
        assertThrows(IllegalArgumentException.class, () -> MatchCommandPolicy.requireStopCommand(id, null, 2L, SESSION, "{}"));
        assertThrows(IllegalArgumentException.class, () -> MatchCommandPolicy.requireStopCommand(id, 0L, 2L, SESSION, "{}"));
        assertThrows(IllegalArgumentException.class, () -> MatchCommandPolicy.requireStopCommand(id, 1L, null, SESSION, "{}"));
        assertThrows(IllegalArgumentException.class, () -> MatchCommandPolicy.requireStopCommand(id, 1L, 0L, SESSION, "{}"));
        assertThrows(IllegalArgumentException.class, () -> MatchCommandPolicy.requireStopCommand(id, 1L, 2L, null, "{}"));
        assertEquals("raw payload is required", assertThrows(IllegalArgumentException.class,
                () -> MatchCommandPolicy.requireStopCommand(id, 1L, 2L, SESSION, " ")).getMessage());
        assertThrows(IllegalArgumentException.class, () -> MatchCommandPolicy.requireStopCommand(id, 1L, 2L, SESSION, null));
    }

    @Test
    void stopSuppressesOnlyUnsentResultsAndCancelsUnpublishedCommands() {
        assertTrue(MatchCommandPolicy.suppressible(OutboxStatus.PENDING));
        assertTrue(MatchCommandPolicy.suppressible(OutboxStatus.DEAD));
        assertFalse(MatchCommandPolicy.suppressible(OutboxStatus.IN_FLIGHT));
        assertFalse(MatchCommandPolicy.suppressible(OutboxStatus.SENT));
        assertFalse(MatchCommandPolicy.suppressible(OutboxStatus.CANCELLED));

        assertTrue(MatchCommandPolicy.cancels(Status.PENDING, false));
        assertTrue(MatchCommandPolicy.cancels(Status.CANDIDATE_STAGED, false));
        assertTrue(MatchCommandPolicy.cancels(Status.RESULT_STAGED, true));
        assertFalse(MatchCommandPolicy.cancels(Status.RESULT_STAGED, false));
        assertFalse(MatchCommandPolicy.cancels(Status.CANCELLED, true));
    }
}
