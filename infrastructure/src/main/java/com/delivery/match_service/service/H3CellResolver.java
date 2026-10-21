package com.delivery.match_service.service;

import com.uber.h3core.H3Core;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.List;

@Service
@Slf4j
public class H3CellResolver {

    private final boolean enabled;
    private final int resolution;
    private H3Core h3;

    public H3CellResolver(
            @Value("${matching.h3.enabled:false}") boolean enabled,
            @Value("${matching.h3.resolution:9}") int resolution) {
        this.enabled = enabled;
        this.resolution = resolution;
    }

    @PostConstruct
    void initialize() {
        if (!enabled) return;
        if (resolution < 0 || resolution > 15) {
            throw new IllegalArgumentException("H3 resolution must be between 0 and 15");
        }
        try {
            h3 = H3Core.newInstance();
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot initialize H3 native binding", exception);
        }
    }

    public String cellFor(Double latitude, Double longitude) {
        if (!enabled || h3 == null || latitude == null || longitude == null
                || !Double.isFinite(latitude) || !Double.isFinite(longitude)) {
            return null;
        }
        return h3.latLngToCellAddress(latitude, longitude, resolution);
    }

    public int resolution() {
        return resolution;
    }

    /** Returns the center cell plus a bounded H3 k-ring for local batching. */
    public List<String> kRing(String cell, int ring) {
        if (!enabled || h3 == null || cell == null || cell.isBlank()) return cell == null ? List.of() : List.of(cell);
        if (ring < 0 || ring > 2) throw new IllegalArgumentException("H3 neighbor ring must be between 0 and 2");
        return h3.gridDisk(cell, ring);
    }
}
