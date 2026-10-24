package com.delivery.saga_orchestrator_service.service;

import com.delivery.saga_orchestrator_service.repository.SagaEarlyEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SagaEarlyEventSchedulerTest {
    @Test
    void delegatesEveryReadyEventToTheDurableManagerPath() {
        SagaEarlyEventRepository repository = mock(SagaEarlyEventRepository.class);
        SagaManager manager = mock(SagaManager.class);
        UUID first = UUID.fromString("11111111-1111-1111-1111-111111111111");
        UUID second = UUID.fromString("22222222-2222-2222-2222-222222222222");
        when(repository.findReadyEventIds(any(Pageable.class))).thenReturn(List.of(first, second));

        new SagaEarlyEventScheduler(repository, manager, 25).processReadyEvents();

        verify(manager).processEarlyEvent(first);
        verify(manager).processEarlyEvent(second);
    }
}
