package com.delivery.match_service.dispatch;

import com.delivery.match.domain.dispatch.DispatchBundleCandidate;
import com.delivery.match.application.api.DispatchMatchingPort;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Bounded assignment optimizer for a dispatch round.
 *
 * The flow stage limits one selected bundle per shipper. Since a pair/triple
 * bundle is a set rather than a simple edge, the selected flow is followed by
 * a deterministic disjointness repair over order IDs. This keeps the hot path
 * bounded while preserving the important invariants.
 */
public final class BoundedDispatchOptimizer implements DispatchMatchingPort {

    private static final long COVERAGE_BONUS = 1_000_000L;

    @Override
    public List<DispatchBundleCandidate> optimize(
            List<DispatchBundleCandidate> candidates,
            int maxAssignments) {
        if (candidates == null || candidates.isEmpty() || maxAssignments <= 0) {
            return List.of();
        }

        List<DispatchBundleCandidate> normalized = candidates.stream()
                .filter(candidate -> candidate.poolItemIds().size() <= 3)
                .sorted(Comparator
                        .comparingLong(this::flowCost)
                        .thenComparing(DispatchBundleCandidate::bundleId))
                .toList();

        List<DispatchBundleCandidate> flowSelection = solveOneBundlePerShipper(
                normalized, Math.min(maxAssignments, distinctShippers(normalized).size()));
        return repairOrderConflicts(flowSelection, normalized, maxAssignments);
    }

    private List<DispatchBundleCandidate> solveOneBundlePerShipper(
            List<DispatchBundleCandidate> candidates,
            int flowLimit) {
        Map<Long, Integer> shipperNodes = new LinkedHashMap<>();
        Map<UUID, Integer> bundleNodes = new LinkedHashMap<>();
        for (DispatchBundleCandidate candidate : candidates) {
            shipperNodes.computeIfAbsent(candidate.shipperId(), ignored -> shipperNodes.size());
            bundleNodes.computeIfAbsent(candidate.bundleId(), ignored -> bundleNodes.size());
        }

        int source = 0;
        int shipperStart = 1;
        int bundleStart = shipperStart + shipperNodes.size();
        int sink = bundleStart + bundleNodes.size();
        FlowNetwork network = new FlowNetwork(sink + 1);

        shipperNodes.values().forEach(node -> network.addEdge(source, shipperStart + node, 1, 0));
        bundleNodes.values().forEach(node -> network.addEdge(bundleStart + node, sink, 1, 0));
        for (DispatchBundleCandidate candidate : candidates) {
            int shipperNode = shipperStart + shipperNodes.get(candidate.shipperId());
            int bundleNode = bundleStart + bundleNodes.get(candidate.bundleId());
            network.addEdge(shipperNode, bundleNode, 1, flowCost(candidate));
        }

        network.minCostFlow(source, sink, flowLimit);
        List<DispatchBundleCandidate> selected = new ArrayList<>();
        for (DispatchBundleCandidate candidate : candidates) {
            int shipperNode = shipperStart + shipperNodes.get(candidate.shipperId());
            int bundleNode = bundleStart + bundleNodes.get(candidate.bundleId());
            if (network.wasUsed(shipperNode, bundleNode)) {
                selected.add(candidate);
            }
        }
        return selected;
    }

    private List<DispatchBundleCandidate> repairOrderConflicts(
            List<DispatchBundleCandidate> selected,
            List<DispatchBundleCandidate> allCandidates,
            int maxAssignments) {
        Comparator<DispatchBundleCandidate> quality = Comparator
                .comparingInt(DispatchBundleCandidate::coveredOrders).reversed()
                .thenComparingLong(DispatchBundleCandidate::scoreMicros)
                .thenComparingLong(DispatchBundleCandidate::routeSeconds)
                .thenComparing(DispatchBundleCandidate::bundleId);

        List<DispatchBundleCandidate> result = new ArrayList<>();
        Set<Long> usedShippers = new HashSet<>();
        Set<UUID> usedOrders = new HashSet<>();
        ArrayDeque<DispatchBundleCandidate> ordered = new ArrayDeque<>(selected.stream().sorted(quality).toList());
        while (!ordered.isEmpty() && result.size() < maxAssignments) {
            DispatchBundleCandidate candidate = ordered.removeFirst();
            if (conflicts(candidate, usedShippers, usedOrders)) {
                continue;
            }
            result.add(candidate);
            usedShippers.add(candidate.shipperId());
            usedOrders.addAll(candidate.poolItemIds());
        }

        for (DispatchBundleCandidate candidate : allCandidates.stream().sorted(quality).toList()) {
            if (result.size() >= maxAssignments || conflicts(candidate, usedShippers, usedOrders)) {
                continue;
            }
            result.add(candidate);
            usedShippers.add(candidate.shipperId());
            usedOrders.addAll(candidate.poolItemIds());
        }
        return result.stream().sorted(quality).toList();
    }

    private boolean conflicts(
            DispatchBundleCandidate candidate,
            Set<Long> usedShippers,
            Set<UUID> usedOrders) {
        if (usedShippers.contains(candidate.shipperId())) {
            return true;
        }
        return candidate.poolItemIds().stream().anyMatch(usedOrders::contains);
    }

    private long flowCost(DispatchBundleCandidate candidate) {
        return candidate.scoreMicros() - COVERAGE_BONUS * candidate.coveredOrders();
    }

    private Set<Long> distinctShippers(List<DispatchBundleCandidate> candidates) {
        Set<Long> ids = new HashSet<>();
        candidates.forEach(candidate -> ids.add(candidate.shipperId()));
        return ids;
    }

    private static final class FlowNetwork {
        private final List<List<Edge>> graph;

        private FlowNetwork(int nodeCount) {
            graph = new ArrayList<>(nodeCount);
            for (int i = 0; i < nodeCount; i++) graph.add(new ArrayList<>());
        }

        private void addEdge(int from, int to, int capacity, long cost) {
            Edge forward = new Edge(to, graph.get(to).size(), capacity, cost, false);
            Edge reverse = new Edge(from, graph.get(from).size(), 0, -cost, true);
            graph.get(from).add(forward);
            graph.get(to).add(reverse);
        }

        private void minCostFlow(int source, int sink, int limit) {
            for (int iteration = 0; iteration < limit; iteration++) {
                long[] distance = new long[graph.size()];
                int[] previousNode = new int[graph.size()];
                int[] previousEdge = new int[graph.size()];
                boolean[] inQueue = new boolean[graph.size()];
                java.util.Arrays.fill(distance, Long.MAX_VALUE / 4);
                java.util.Arrays.fill(previousNode, -1);
                java.util.Arrays.fill(previousEdge, -1);
                distance[source] = 0;
                ArrayDeque<Integer> queue = new ArrayDeque<>();
                queue.add(source);
                inQueue[source] = true;
                while (!queue.isEmpty()) {
                    int node = queue.removeFirst();
                    inQueue[node] = false;
                    for (int edgeIndex = 0; edgeIndex < graph.get(node).size(); edgeIndex++) {
                        Edge edge = graph.get(node).get(edgeIndex);
                        if (edge.capacity <= 0 || distance[edge.to] <= distance[node] + edge.cost) continue;
                        distance[edge.to] = distance[node] + edge.cost;
                        previousNode[edge.to] = node;
                        previousEdge[edge.to] = edgeIndex;
                        if (!inQueue[edge.to]) {
                            queue.addLast(edge.to);
                            inQueue[edge.to] = true;
                        }
                    }
                }
                if (previousNode[sink] < 0) return;
                for (int node = sink; node != source; node = previousNode[node]) {
                    Edge edge = graph.get(previousNode[node]).get(previousEdge[node]);
                    edge.capacity--;
                    graph.get(node).get(edge.reverseIndex).capacity++;
                }
            }
        }

        private boolean wasUsed(int from, int to) {
            return graph.get(from).stream().anyMatch(edge -> edge.to == to && edge.reverseIndex >= 0
                    && graph.get(to).get(edge.reverseIndex).capacity > 0);
        }

        private static final class Edge {
            private final int to;
            private final int reverseIndex;
            private int capacity;
            private final long cost;
            @SuppressWarnings("unused")
            private final boolean reverse;

            private Edge(int to, int reverseIndex, int capacity, long cost, boolean reverse) {
                this.to = to;
                this.reverseIndex = reverseIndex;
                this.capacity = capacity;
                this.cost = cost;
                this.reverse = reverse;
            }
        }
    }
}
