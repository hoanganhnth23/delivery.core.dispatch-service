package com.delivery.match.application.api;

import com.delivery.match.domain.dispatch.DispatchBundleCandidate;

import java.util.List;

/** Application boundary for selecting dispatch assignments. */
public interface DispatchMatchingPort {
    List<DispatchBundleCandidate> optimize(List<DispatchBundleCandidate> candidates, int maxAssignments);
}
