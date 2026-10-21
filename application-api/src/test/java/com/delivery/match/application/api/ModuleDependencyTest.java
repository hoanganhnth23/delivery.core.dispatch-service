package com.delivery.match.application.api;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;

class ModuleDependencyTest {
    @Test
    void applicationApiSourceDoesNotImportFrameworkTypes() throws Exception {
        try (var files = Files.walk(Path.of("src/main/java"))) {
            assertFalse(files.filter(Files::isRegularFile)
                    .flatMap(path -> {
                        try { return Files.readAllLines(path).stream(); }
                        catch (Exception e) { throw new RuntimeException(e); }
                    })
                    .anyMatch(line -> line.contains("org.springframework") || line.contains("jakarta.persistence")));
        }
    }
}
