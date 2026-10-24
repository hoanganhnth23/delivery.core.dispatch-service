package com.delivery.saga_orchestrator_service.service;

import com.delivery.dispatch.domain.CaseHistory;
import com.delivery.saga_orchestrator_service.entity.SagaInstance;
import com.delivery.saga_orchestrator_service.entity.SagaStep;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** {@link CaseHistory} over the legacy saga_steps rows and their JSON event data. */
final class JsonCaseHistory implements CaseHistory {

    private final List<SagaStep> steps;
    private final ObjectMapper objectMapper;

    JsonCaseHistory(SagaInstance saga, ObjectMapper objectMapper) {
        this.steps = saga.getSteps() == null ? List.of() : saga.getSteps();
        this.objectMapper = objectMapper;
    }

    @Override
    public boolean has(String stepName) {
        return steps.stream().anyMatch(step -> stepName.equals(step.getStepName()));
    }

    @Override
    public String latest(String stepName) {
        String data = null;
        for (SagaStep step : steps) {
            if (stepName.equals(step.getStepName()) && step.getEventData() != null) {
                data = step.getEventData();
            }
        }
        return data;
    }

    @Override
    public String latestWithPrefix(String stepPrefix) {
        String data = null;
        for (SagaStep step : steps) {
            if (step.getStepName().startsWith(stepPrefix) && step.getEventData() != null) {
                data = step.getEventData();
            }
        }
        return data;
    }

    @Override
    public Fact latestFact(String stepName) {
        for (int i = steps.size() - 1; i >= 0; i--) {
            SagaStep step = steps.get(i);
            if (stepName.equals(step.getStepName())) {
                return new Fact(step.getEventData(), step.getExecutedAt());
            }
        }
        return null;
    }

    @Override
    public String latestField(String stepName, String field) {
        String data = latest(stepName);
        if (data == null) return null;
        try {
            JsonNode value = objectMapper.readTree(data).path(field);
            return value.isMissingNode() || value.isNull() ? null : value.asText();
        } catch (Exception unreadable) {
            return null;
        }
    }

    @Override
    public long countWithPrefix(String stepPrefix) {
        return steps.stream().filter(step -> step.getStepName().startsWith(stepPrefix)).count();
    }

    @Override
    public long count(String stepName) {
        return steps.stream().filter(step -> stepName.equals(step.getStepName())).count();
    }

    @Override
    public List<Long> rejectingShippers() {
        List<Long> rejected = new ArrayList<>();
        for (SagaStep step : steps) {
            if (step.getStepName().startsWith("SHIPPER_REJECTED")) {
                addRejected(step, rejected);
            }
        }
        return rejected;
    }

    @Override
    public List<Long> recordedRejectedShippers() {
        List<Long> rejected = new ArrayList<>();
        for (SagaStep step : steps) {
            addRejected(step, rejected);
        }
        return rejected;
    }

    private void addRejected(SagaStep step, List<Long> target) {
        if (step.getEventData() == null) return;
        try {
            JsonNode event = objectMapper.readTree(step.getEventData());
            if (event.hasNonNull("rejectedShipperId")) {
                target.add(event.get("rejectedShipperId").asLong());
            }
        } catch (Exception ignored) {
            // A malformed historic step remains visible but contributes no exclusion.
        }
    }

    @Override
    public UUID currentMatchingSession() {
        String matchingStart = latest("MATCHING_STARTED");
        if (matchingStart == null) {
            return null;
        }
        try {
            JsonNode payload = objectMapper.readTree(matchingStart);
            if (!payload.hasNonNull("matchingSessionId")) {
                // Pre-contract active cases cannot safely target a generation.
                return null;
            }
            return UUID.fromString(payload.get("matchingSessionId").asText());
        } catch (Exception malformed) {
            throw new IllegalStateException("Persisted matching session identity is malformed", malformed);
        }
    }
}
