package com.delivery.match.domain.batch;

import com.delivery.match.domain.batch.BatchBundlePolicy.Point;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class BatchBundlePolicyTest {

    private static final Point HCM = new Point(10.7769, 106.7009);
    private static final Point HCM_1KM = new Point(10.7859, 106.7009);
    private static final Point HCM_3KM = new Point(10.8039, 106.7009);

    private static UUID name(String value) {
        return UUID.nameUUIDFromBytes(value.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void admitsOnlyCodItemsWithCoordinatesWithinWaveBudget() {
        assertTrue(BatchBundlePolicy.admits(0, 3, "cod", HCM, HCM_1KM));
        assertFalse(BatchBundlePolicy.admits(3, 3, "COD", HCM, HCM_1KM));
        assertFalse(BatchBundlePolicy.admits(1, 0, "COD", HCM, HCM_1KM));
        assertTrue(BatchBundlePolicy.admits(0, 0, "COD", HCM, HCM_1KM));
        assertFalse(BatchBundlePolicy.admits(0, 3, "ONLINE", HCM, HCM_1KM));
        assertFalse(BatchBundlePolicy.admits(0, 3, null, HCM, HCM_1KM));
        assertFalse(BatchBundlePolicy.admits(0, 3, "COD", new Point(null, 1.0), HCM));
        assertFalse(BatchBundlePolicy.admits(0, 3, "COD", new Point(1.0, null), HCM));
        assertFalse(BatchBundlePolicy.admits(0, 3, "COD", HCM, new Point(null, 1.0)));
        assertFalse(BatchBundlePolicy.admits(0, 3, "COD", HCM, new Point(1.0, null)));
    }

    @Test
    void distanceIsHaversineAndUnknownCoordinatesAreInfinitelyFar() {
        assertEquals(0.0, BatchBundlePolicy.distanceKm(HCM, HCM), 1e-9);
        assertEquals(1.0, BatchBundlePolicy.distanceKm(HCM, HCM_1KM), 0.01);
        assertEquals(Double.MAX_VALUE, BatchBundlePolicy.distanceKm(new Point(null, 1.0), HCM));
        assertEquals(Double.MAX_VALUE, BatchBundlePolicy.distanceKm(new Point(1.0, null), HCM));
        assertEquals(Double.MAX_VALUE, BatchBundlePolicy.distanceKm(HCM, new Point(null, 1.0)));
        assertEquals(Double.MAX_VALUE, BatchBundlePolicy.distanceKm(HCM, new Point(1.0, null)));
    }

    @Test
    void pickupsMustBePairwiseWithinTwoKilometres() {
        assertTrue(BatchBundlePolicy.pickupsFeasible(List.of()));
        assertTrue(BatchBundlePolicy.pickupsFeasible(List.of(HCM)));
        assertTrue(BatchBundlePolicy.pickupsFeasible(List.of(HCM, HCM_1KM)));
        assertFalse(BatchBundlePolicy.pickupsFeasible(List.of(HCM, HCM_1KM, HCM_3KM)));
    }

    @Test
    void seedsAreNearestFirstTieBrokenByIdentityAndBounded() {
        UUID a = UUID.fromString("00000000-0000-0000-0000-00000000000a");
        UUID b = UUID.fromString("00000000-0000-0000-0000-00000000000b");
        UUID c = UUID.fromString("00000000-0000-0000-0000-00000000000c");
        Map<UUID, Double> distance = Map.of(a, 2.0, b, 1.0, c, 1.0);
        assertEquals(List.of(b, c), BatchBundlePolicy.seeds(List.of(a, b, c), distance::get, 2));
        assertEquals(List.of(b), BatchBundlePolicy.seeds(List.of(a, b, c), distance::get, 0));
    }

    @Test
    void bundlesEnumerateSingletonsFromAllAndGroupsFromSeedsOnly() {
        UUID a = UUID.fromString("00000000-0000-0000-0000-00000000000a");
        UUID b = UUID.fromString("00000000-0000-0000-0000-00000000000b");
        UUID c = UUID.fromString("00000000-0000-0000-0000-00000000000c");
        UUID d = UUID.fromString("00000000-0000-0000-0000-00000000000d");
        List<List<UUID>> emitted = new ArrayList<>();
        BatchBundlePolicy.bundles(List.of(a, b, c, d), List.of(a, b, c), emitted::add);
        assertEquals(List.of(List.of(a), List.of(b), List.of(c), List.of(d),
                List.of(a, b), List.of(a, c), List.of(b, c), List.of(a, b, c)), emitted);
        List<List<UUID>> single = new ArrayList<>();
        BatchBundlePolicy.bundles(List.of(a), List.of(a), single::add);
        assertEquals(List.of(List.of(a)), single);
    }

    @Test
    void detourAndScoreKeepLegacyWeights() {
        assertEquals(0, BatchBundlePolicy.incrementalSeconds(100, 200));
        assertEquals(50, BatchBundlePolicy.incrementalSeconds(250, 200));
        assertTrue(BatchBundlePolicy.withinDetour(600, 600));
        assertFalse(BatchBundlePolicy.withinDetour(601, 600));
        assertEquals(250_000L + 5_000L, BatchBundlePolicy.score(250, 50));
    }

    @Test
    void identitiesMatchLegacyNameBasedUuids() {
        UUID round = UUID.fromString("11111111-1111-1111-1111-111111111111");
        UUID order = UUID.fromString("22222222-2222-2222-2222-222222222222");
        UUID bundle = BatchBundlePolicy.bundleId(7L, List.of(order));
        assertEquals(name("bundle:7:[" + order + "]"), bundle);
        UUID batch = BatchBundlePolicy.batchId(round, 7L, bundle);
        assertEquals(name("dispatch-batch:" + round + ":7:" + bundle), batch);
        assertEquals(name("cod-hold:" + batch + ":9"), BatchBundlePolicy.codHoldId(batch, 9L));
        assertEquals(name("cod-offer:" + batch + ":9"), BatchBundlePolicy.codOfferId(batch, 9L));
        assertEquals(name("shipper-found:" + batch + ":9"), BatchBundlePolicy.shipperFoundEventId(batch, 9L));
    }

    @Test
    void stopSequencePreservesScoredInterleavingAndTimeoutIsClamped() {
        assertEquals(new BatchBundlePolicy.StopSequence(0, 1), BatchBundlePolicy.stopSequence(0, 3));
        assertEquals(new BatchBundlePolicy.StopSequence(2, 3), BatchBundlePolicy.stopSequence(1, 3));
        assertEquals(new BatchBundlePolicy.StopSequence(4, 5), BatchBundlePolicy.stopSequence(2, 3));
        assertEquals(new BatchBundlePolicy.StopSequence(0, 1), BatchBundlePolicy.stopSequence(0, 1));
        assertThrows(IllegalArgumentException.class, () -> BatchBundlePolicy.stopSequence(3, 3));
        assertThrows(IllegalArgumentException.class, () -> BatchBundlePolicy.stopSequence(-1, 3));
        assertEquals(1, BatchBundlePolicy.waitingTimeoutSeconds(0));
        assertEquals(20, BatchBundlePolicy.waitingTimeoutSeconds(20));
        assertEquals(180, BatchBundlePolicy.waitingTimeoutSeconds(999));
    }
}
