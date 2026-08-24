package org.example.Utility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

/**
 * The strict loader over YAML: the repo's local config must load and
 * validate, and the unknown-key typo protection must work through the YAML
 * path exactly as it does for JSON.
 */
class ExperimentConfigTest {

    // Surefire's working directory is the build directory, a direct child of
    // the repo root, so the repo config is one level up.
    private static final Path REPO_CONFIG = Path.of("..", "config_local.yaml");

    @Test
    void repoConfigLoadsAndValidates() {
        assertTrue(Files.exists(REPO_CONFIG), "repo config_local.yaml must exist at " + REPO_CONFIG.toAbsolutePath());
        ExperimentConfig config = ExperimentConfig.load(REPO_CONFIG);
        assertEquals(config.cluster.serverHosts.size(), (int) config.cluster.numServers);
        assertTrue(config.slas.size() >= 1);
        assertTrue(config.server.maxEntriesPerReplicationBatch > 0);
        assertTrue(config.server.maxInflightReplicationBatchesPerFollower > 0);
        // The mode helpers agree with the mode string.
        assertEquals(config.mode.startsWith("chameleon"), config.chameleonDecision());
    }

    @Test
    void unknownKeysAreRejectedThroughYaml() throws IOException {
        String yaml = Files.readString(REPO_CONFIG) + "\nnotARealKey: 42\n";
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, yaml);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains("notARealKey"), e.getMessage());
        } finally {
            Files.delete(tmp);
        }
    }

    @Test
    void mixEntriesMustReferenceRegisteredSlas() throws IOException {
        // Point one mix entry at an application that registers no SLAs. The
        // pattern matches only uncommented entries (a comment line cannot
        // start with "- {"), so the test holds regardless of which entries
        // the repo config currently has commented out.
        String yaml = Files.readString(REPO_CONFIG)
                .replaceFirst("(?m)^(\\s*)- \\{ applicationId: \\d+", "$1- { applicationId: 9");
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, yaml);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains("unregistered SLA 9/"), e.getMessage());
        } finally {
            Files.delete(tmp);
        }
    }

    @Test
    void phasesMustReferenceDefinedMixes() throws IOException {
        // workload.phases is the last section of the repo config, so an
        // appended list item (at its indentation) extends it with one more phase.
        String yaml = Files.readString(REPO_CONFIG)
                + "    - name: Bad\n"
                + "      durationSeconds: 10\n"
                + "      totalTPS: 100\n"
                + "      mix: doesNotExist\n";
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, yaml);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains("doesNotExist"), e.getMessage());
        } finally {
            Files.delete(tmp);
        }
    }

    @Test
    void invalidValuesFailFast() throws IOException {
        String yaml = Files.readString(REPO_CONFIG).replaceFirst("mode: \\w+", "mode: turbo");
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, yaml);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains("mode"), e.getMessage());
        } finally {
            Files.delete(tmp);
        }
    }

    @Test
    void clientSiteLatencyRowsMustMatchNumServers() throws IOException {
        // Drop the first entry of the first site's latency row (the only
        // "latencyMs: [" occurrences in the config are the site rows).
        String original = Files.readString(REPO_CONFIG);
        String yaml = original.replaceFirst("latencyMs: \\[[^,]+, ", "latencyMs: [");
        assertTrue(!yaml.equals(original), "test setup: the replacement must have matched a site row");
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, yaml);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains("latencyMs"), e.getMessage());
        } finally {
            Files.delete(tmp);
        }
    }

    @Test
    void sessionsMustSpreadEvenlyAcrossClientSites() throws IOException {
        // The repo config's session count divides its site count; one more
        // session breaks the even spread and must be rejected explicitly.
        ExperimentConfig valid = ExperimentConfig.load(REPO_CONFIG);
        int uneven = valid.workload.sessionsPerApplication + 1;
        String yaml = Files.readString(REPO_CONFIG)
                .replaceFirst("sessionsPerApplication: \\d+", "sessionsPerApplication: " + uneven)
                .replaceFirst("injectorThreads: \\d+", "injectorThreads: 1");
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, yaml);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains("divisible"), e.getMessage());
        } finally {
            Files.delete(tmp);
        }
    }

    @Test
    void clientSiteBindHostsFollowTheFixedConvention() {
        assertEquals("127.0.2.1", ExperimentConfig.clientSiteBindHost(0));
        assertEquals("127.0.2.6", ExperimentConfig.clientSiteBindHost(5));
        assertThrows(IllegalArgumentException.class, () -> ExperimentConfig.clientSiteBindHost(-1));
        assertThrows(IllegalArgumentException.class, () -> ExperimentConfig.clientSiteBindHost(254));
    }
}
