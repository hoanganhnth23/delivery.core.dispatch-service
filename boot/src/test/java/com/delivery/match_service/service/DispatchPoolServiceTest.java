package com.delivery.match_service.service;

import com.delivery.match_service.config.MatchingBatchProperties;
import com.delivery.match_service.dto.event.FindShipperEvent;
import com.delivery.match_service.entity.DispatchPoolItem;
import com.delivery.match_service.repository.DispatchPoolItemRepository;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class DispatchPoolServiceTest {
    private final DispatchPoolItemRepository repository = mock(DispatchPoolItemRepository.class);
    private final H3CellResolver cells = mock(H3CellResolver.class);
    private final MatchingBatchProperties properties = new MatchingBatchProperties();
    private final DispatchPoolService service = new DispatchPoolService(repository, properties, cells);

    @Test
    void newGenerationPersistsCoordinatesPaymentAndExplicitDeadlineAndZone() {
        properties.setEnabled(true);
        FindShipperEvent event = event();
        event.setMatchingDeadlineAt(LocalDateTime.now().plusMinutes(2));
        event.setBatchWave(2);
        LocalDateTime before = LocalDateTime.now();
        UUID id = service.enqueue(event, "explicit-zone");
        ArgumentCaptor<DispatchPoolItem> saved = ArgumentCaptor.forClass(DispatchPoolItem.class);
        verify(repository).saveAndFlush(saved.capture());
        DispatchPoolItem item = saved.getValue();
        assertThat(item.getPoolItemId()).isEqualTo(id);
        assertThat(item.getOrderId()).isEqualTo(event.getOrderId());
        assertThat(item.getDeliveryId()).isEqualTo(event.getDeliveryId());
        assertThat(item.getMatchingSessionId()).isEqualTo(event.getMatchingSessionId());
        assertThat(item.getPickupH3Cell()).isEqualTo("explicit-zone");
        assertThat(item.getPickupLat()).isEqualTo(event.getPickupLat());
        assertThat(item.getPickupLng()).isEqualTo(event.getPickupLng());
        assertThat(item.getDeliveryLat()).isEqualTo(event.getDeliveryLat());
        assertThat(item.getDeliveryLng()).isEqualTo(event.getDeliveryLng());
        assertThat(item.getTotalPrice()).isEqualByComparingTo(event.getTotalPrice());
        assertThat(item.getPaymentMethod()).isEqualTo("COD");
        assertThat(item.getMatchingDeadlineAt()).isEqualTo(event.getMatchingDeadlineAt());
        assertThat(item.getWaveNumber()).isEqualTo(2);
        assertThat(item.getState()).isEqualTo(DispatchPoolItem.State.WAITING);
        assertThat(item.getVersion()).isZero();
        assertThat(item.getCreatedAt()).isBetween(before, LocalDateTime.now());
        assertThat(item.getEligibleAt()).isEqualTo(item.getCreatedAt());
        assertThat(item.getUpdatedAt()).isEqualTo(item.getCreatedAt());
        verifyNoInteractions(cells);
    }

    @Test
    void missingDeadlineAndZoneUseFiveMinuteWindowAndResolvedPickupCell() {
        properties.setEnabled(true);
        FindShipperEvent event = event();
        when(cells.cellFor(event.getPickupLat(), event.getPickupLng())).thenReturn("resolved-zone");
        service.enqueue(event, null);
        ArgumentCaptor<DispatchPoolItem> saved = ArgumentCaptor.forClass(DispatchPoolItem.class);
        verify(repository).saveAndFlush(saved.capture());
        assertThat(saved.getValue().getPickupH3Cell()).isEqualTo("resolved-zone");
        assertThat(saved.getValue().getMatchingDeadlineAt()).isEqualTo(saved.getValue().getCreatedAt().plusMinutes(5));
        assertThat(saved.getValue().getWaveNumber()).isZero();
    }

    @Test
    void duplicateGenerationReturnsExistingIdentityWithoutOverwritingOrResolvingZone() {
        properties.setEnabled(true);
        FindShipperEvent event = event();
        DispatchPoolItem existing = new DispatchPoolItem();
        existing.setPoolItemId(UUID.randomUUID());
        existing.setState(DispatchPoolItem.State.ASSIGNED);
        when(repository.findByDeliveryAndSessionForUpdate(event.getDeliveryId(), event.getMatchingSessionId()))
                .thenReturn(Optional.of(existing));
        assertThat(service.enqueue(event, null)).isEqualTo(existing.getPoolItemId());
        assertThat(existing.getState()).isEqualTo(DispatchPoolItem.State.ASSIGNED);
        verify(repository, never()).saveAndFlush(any());
        verifyNoInteractions(cells);
    }

    @Test
    void invalidOrDisabledIntakeCannotTouchPersistence() {
        assertThatThrownBy(() -> service.enqueue(null, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.enqueue(new FindShipperEvent(), null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.enqueue(event(), null)).isInstanceOf(IllegalStateException.class)
                .hasMessage("Rolling batch dispatch is disabled");
        verifyNoInteractions(repository, cells);
    }

    private FindShipperEvent event() {
        FindShipperEvent event = new FindShipperEvent();
        event.setOrderId(11L); event.setDeliveryId(12L); event.setMatchingSessionId(UUID.randomUUID());
        event.setPickupLat(10.76); event.setPickupLng(106.66);
        event.setDeliveryLat(10.78); event.setDeliveryLng(106.68);
        event.setTotalPrice(new BigDecimal("120000")); event.setPaymentMethod("COD");
        return event;
    }
}
