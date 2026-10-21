package com.delivery.match_service.service;

import com.delivery.match_service.config.MatchingBatchProperties;
import com.delivery.match_service.entity.DispatchPoolItem;
import com.delivery.match_service.entity.DispatchRound;
import com.delivery.match_service.repository.DispatchPoolItemRepository;
import com.delivery.match_service.repository.DispatchRoundRepository;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Pageable;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class DispatchRoundServiceTest {
    private final DispatchPoolItemRepository pool = mock(DispatchPoolItemRepository.class);
    private final DispatchRoundRepository rounds = mock(DispatchRoundRepository.class);
    private final H3CellResolver cells = mock(H3CellResolver.class);
    private final MatchingBatchProperties properties = new MatchingBatchProperties();
    private final DispatchRoundService service = new DispatchRoundService(pool, rounds, properties, cells);

    @Test
    void disabledInvalidAndAlreadyOpenZonesCannotClaimItems() {
        assertThatThrownBy(() -> service.openAndClaim(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.openAndClaim(" ")).isInstanceOf(IllegalArgumentException.class);
        assertThat(service.openAndClaim("zone")).isEmpty();
        verifyNoInteractions(pool, rounds, cells);
        properties.setEnabled(true);
        when(rounds.findFirstByH3ZoneAndStateOrderByOpenedAtAsc("zone", DispatchRound.State.OPEN))
                .thenReturn(Optional.of(new DispatchRound()));
        assertThat(service.openAndClaim("zone")).isEmpty();
        verifyNoInteractions(pool, cells);
    }

    @Test
    void centerOnlyClaimsBoundedRoundAndIncrementsEveryItemVersion() {
        properties.setEnabled(true);
        properties.setMaxOrdersPerRound(1000);
        properties.setWindowSeconds(0);
        when(cells.kRing("zone", properties.getNeighborRing())).thenReturn(List.of("zone"));
        DispatchPoolItem first = new DispatchPoolItem(); first.setVersion(4);
        DispatchPoolItem second = new DispatchPoolItem(); second.setVersion(9);
        when(pool.findReadyByZoneForUpdate(eq("zone"), any(), any())).thenAnswer(call -> {
            assertThat(((Pageable) call.getArgument(2)).getPageSize()).isEqualTo(500);
            return List.of(first, second);
        });
        LocalDateTime before = LocalDateTime.now();
        var claimed = service.openAndClaim("zone").orElseThrow();
        DispatchRound round = claimed.round();
        assertThat(round.getState()).isEqualTo(DispatchRound.State.OPEN);
        assertThat(round.getH3Zone()).isEqualTo("zone");
        assertThat(round.getOrderCount()).isEqualTo(2);
        assertThat(round.getShipperCount()).isZero();
        assertThat(round.getOpenedAt()).isBetween(before, LocalDateTime.now());
        assertThat(round.getCutoffAt()).isEqualTo(round.getOpenedAt().plusSeconds(1));
        assertThat(round.getCreatedAt()).isEqualTo(round.getOpenedAt());
        assertThat(round.getUpdatedAt()).isEqualTo(round.getOpenedAt());
        assertThat(claimed.items()).containsExactly(first, second);
        assertThat(first.getVersion()).isEqualTo(5);
        assertThat(second.getVersion()).isEqualTo(10);
        for (DispatchPoolItem item : claimed.items()) {
            assertThat(item.getState()).isEqualTo(DispatchPoolItem.State.CLAIMED);
            assertThat(item.getClaimedRoundId()).isEqualTo(round.getDispatchRoundId());
            assertThat(item.getUpdatedAt()).isEqualTo(round.getOpenedAt());
        }
        verify(rounds).save(round);
        verify(pool).saveAll(claimed.items());
        verify(pool, never()).findReadyByZonesForUpdate(any(), any(), any());
    }

    @Test
    void neighborSearchUsesAllCellsAndEmptyPoolDoesNotCreateRound() {
        properties.setEnabled(true);
        properties.setMaxOrdersPerRound(-1);
        List<String> zones = List.of("zone", "neighbor");
        when(cells.kRing("zone", properties.getNeighborRing())).thenReturn(zones);
        when(pool.findReadyByZonesForUpdate(eq(zones), any(), any())).thenAnswer(call -> {
            assertThat(((Pageable) call.getArgument(2)).getPageSize()).isEqualTo(1);
            return List.of();
        });
        assertThat(service.openAndClaim("zone")).isEmpty();
        verify(rounds, never()).save(any());
        verify(pool, never()).saveAll(any());
        verify(pool, never()).findReadyByZoneForUpdate(any(), any(), any());
    }
}
