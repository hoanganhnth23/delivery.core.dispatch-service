package com.delivery.match_service.service;

import org.junit.jupiter.api.Test;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class H3CellResolverTest {
    @Test
    void disabledAndUninitializedResolversPreserveFallbackCell() {
        for (H3CellResolver resolver : List.of(new H3CellResolver(false, -1), new H3CellResolver(true, 9))) {
            assertThat(resolver.cellFor(10.0, 106.0)).isNull();
            assertThat(resolver.kRing(null, 3)).isEmpty();
            assertThat(resolver.kRing("cell", -1)).containsExactly("cell");
        }
        new H3CellResolver(false, -1).initialize();
    }

    @Test
    void validatesResolutionBeforeLoadingNativeBinding() {
        for (int resolution : List.of(-1, 16)) {
            assertThatThrownBy(() -> new H3CellResolver(true, resolution).initialize())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("H3 resolution must be between 0 and 15");
        }
    }

    @Test
    void validatesCoordinatesAndNeighborBoundsWithNativeCells() {
        for (int resolution : List.of(0, 15)) {
            H3CellResolver resolver = new H3CellResolver(true, resolution);
            resolver.initialize();
            assertThat(resolver.resolution()).isEqualTo(resolution);
            assertThat(resolver.cellFor(null, 106.0)).isNull();
            assertThat(resolver.cellFor(10.0, null)).isNull();
            for (double invalid : new double[]{Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
                assertThat(resolver.cellFor(invalid, 106.0)).isNull();
                assertThat(resolver.cellFor(10.0, invalid)).isNull();
            }
            assertThat(resolver.kRing(null, 0)).isEmpty();
            assertThat(resolver.kRing("", 0)).containsExactly("");
            assertThat(resolver.kRing(" ", 0)).containsExactly(" ");
            String cell = resolver.cellFor(10.0, 106.0);
            assertThat(cell).isNotBlank();
            assertThat(resolver.kRing(cell, 0)).containsExactly(cell);
            assertThat(resolver.kRing(cell, 2)).contains(cell).hasSize(19);
            for (int ring : List.of(-1, 3)) {
                assertThatThrownBy(() -> resolver.kRing(cell, ring))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("H3 neighbor ring must be between 0 and 2");
            }
        }
    }
}
