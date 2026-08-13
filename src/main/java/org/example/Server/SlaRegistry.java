package org.example.Server;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.example.Utility.ExperimentConfig;
import org.example.raft.ReadLevel;

/**
 * Registered SLA tables: (application id, SLA id) -> a set of rungs. Rung
 * strengths use the read-level ladder ordinal for reads (EVENTUAL_LOCAL <
 * EVENTUAL_MAJORITY < CAUSAL_LOCAL < CAUSAL_MAJORITY < LINEARIZABLE, the
 * spec's total order) and the write concern itself for writes. Requests carry
 * only (applicationId, slaId); everything about value lives here.
 *
 * Populated from config at startup; tests register programmatically.
 */
public final class SlaRegistry {

    private SlaRegistry() {
    }

    private record Key(int applicationId, int slaId) {
    }

    private static final Map<Key, List<RungScorer.Rung>> readSlas = new ConcurrentHashMap<>();
    private static final Map<Key, List<RungScorer.Rung>> writeSlas = new ConcurrentHashMap<>();

    public static void applyConfig(ExperimentConfig config) {
        clear();
        for (ExperimentConfig.AppSlas app : config.slas) {
            for (ExperimentConfig.Sla sla : app.read) {
                List<RungScorer.Rung> rungs = sla.rungs.stream()
                        .map(r -> new RungScorer.Rung(ReadLevel.valueOf(r.level).getNumber(), r.latencyMs, r.profit))
                        .toList();
                registerReadSla(app.applicationId, sla.slaId, rungs);
            }
            for (ExperimentConfig.Sla sla : app.write) {
                List<RungScorer.Rung> rungs = sla.rungs.stream()
                        .map(r -> new RungScorer.Rung(r.concern, r.latencyMs, r.profit))
                        .toList();
                registerWriteSla(app.applicationId, sla.slaId, rungs);
            }
        }
    }

    public static void registerReadSla(int applicationId, int slaId, List<RungScorer.Rung> rungs) {
        readSlas.put(new Key(applicationId, slaId), List.copyOf(rungs));
    }

    public static void registerWriteSla(int applicationId, int slaId, List<RungScorer.Rung> rungs) {
        writeSlas.put(new Key(applicationId, slaId), List.copyOf(rungs));
    }

    /** null when unregistered: the request is malformed and fails explicitly. */
    public static List<RungScorer.Rung> readSla(int applicationId, int slaId) {
        return readSlas.get(new Key(applicationId, slaId));
    }

    public static List<RungScorer.Rung> writeSla(int applicationId, int slaId) {
        return writeSlas.get(new Key(applicationId, slaId));
    }

    public static void clear() {
        readSlas.clear();
        writeSlas.clear();
    }
}
