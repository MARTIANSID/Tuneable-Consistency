package org.example.Utility;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

/**
 * Experiment configuration, loaded from a JSON file at startup.
 *
 * Loading is strict by design: a missing file, malformed JSON, an unknown key
 * (usually a typo), or an invalid value all fail fast with an explicit error
 * instead of silently falling back to defaults.
 */
public final class ExperimentConfig {

    // --- Schema (wrapper types so that missing keys are detectable as null) ---

    public Cluster cluster;
    public Chameleon chameleon;
    public List<AppSlas> slas;
    public NodeFailure nodeFailure;
    public TimedFailure timedFailure;
    public Geo geo;
    public Experiment experiment;
    public List<PhaseConfig> phases;

    public static final class Cluster {
        public Integer numServers;
        public List<String> serverHosts; // one entry per server
        public Integer serverBasePort;   // server i listens on basePort + i + 1
    }

    public static final class Chameleon {
        public Integer maxWaitMs;          // bound on every server-side wait; expiry falls back to no-wait level
        public Integer clientLostTimeoutMs; // no response within this -> the client scores the request as lost
        public Integer rttWindowSize;      // per-node sliding window of RTT samples (no-wait replies only)
        public Integer clientRetryLimit;   // redirect/failure resends before giving up on a request
        public Double sMax;                // occupancy slot budget (avg requests in flight); placeholder until the stage 6 load sweep
        public Integer controlIntervalMs;  // utilization / price-controller interval
        public Double replicationBudgetPerSecond; // leader replication rate budget in entries/s; placeholder until stage 6
        public Double uTarget;             // price controller utilization target (~0.85)
        public Double eta;                 // price controller gain (~1)
        public Double lambdaMin;           // price floor; only its order of magnitude matters
    }

    /** SLAs registered per application; requests carry only (applicationId, slaId). */
    public static final class AppSlas {
        public Integer applicationId;
        public List<Sla> read;
        public List<Sla> write;
    }

    public static final class Sla {
        public Integer slaId;
        public List<SlaRung> rungs;
    }

    /**
     * One rung (kappa, delta, pi). Read rungs set level (a ReadLevel name),
     * write rungs set concern (1..majority); exactly one of the two.
     */
    public static final class SlaRung {
        public String level;
        public Integer concern;
        public Double latencyMs;
        public Double profit;
    }

    public static final class NodeFailure {
        public Boolean enabled;    // drop all inter-server RPCs on one node
        public Integer failedNodeId;
    }

    public static final class TimedFailure {
        public Boolean enabled;      // fail one node N seconds after start
        public String targetRole;    // LEADER or FOLLOWER
        public Integer afterSeconds;
    }

    public static final class Geo {
        public Boolean enabled;      // Linux tc/netem only
        public Integer latencyMs;
        public String scriptPath;
        public Integer scriptTimeoutSeconds;
        public Boolean clearOnExit;
        public Boolean useSudo;
    }

    public static final class Experiment {
        public Integer bufferSeconds;      // added to the phase-duration sum for the hard deadline
        public Boolean runSinglePhase;     // run one phase for the whole experiment
        public Integer singlePhaseIndex;   // 0-based, used when runSinglePhase
        public Integer singlePhaseDurationSeconds; // duration when runSinglePhase
    }

    /**
     * Phases control load and read/write mix only. The consistency-level mix
     * is no longer configured anywhere: the server chooses levels from the
     * registered SLAs and the current price.
     */
    public static final class PhaseConfig {
        public String name;
        public Integer durationSeconds;
        public Integer totalTPS;
        public Double readPercentage;
        public Double writePercentage;
    }

    // --- Loading ---

    public static ExperimentConfig load(Path path) {
        String json;
        try {
            json = Files.readString(path);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot read config file: " + path.toAbsolutePath(), e);
        }

        JsonElement root;
        try {
            root = JsonParser.parseString(json);
        } catch (JsonSyntaxException e) {
            throw new IllegalArgumentException("Config file is not valid JSON: " + path.toAbsolutePath() + " - " + e.getMessage(), e);
        }
        if (!root.isJsonObject()) {
            throw new IllegalArgumentException("Config root must be a JSON object: " + path.toAbsolutePath());
        }

        rejectUnknownKeys(root.getAsJsonObject(), ExperimentConfig.class, "");

        ExperimentConfig config = new Gson().fromJson(root, ExperimentConfig.class);
        config.validate();
        return config;
    }

    /** Recursively reject keys that do not correspond to a schema field (typo protection). */
    private static void rejectUnknownKeys(JsonObject obj, Class<?> cls, String prefix) {
        Set<String> known = Arrays.stream(cls.getDeclaredFields()).map(Field::getName).collect(Collectors.toSet());
        for (String key : obj.keySet()) {
            if (!known.contains(key)) {
                throw new IllegalArgumentException("Unknown config key: '" + prefix + key + "'. Known keys at this level: " + known);
            }
            Field field;
            try {
                field = cls.getDeclaredField(key);
            } catch (NoSuchFieldException e) {
                throw new AssertionError(e);
            }
            JsonElement value = obj.get(key);
            if (value.isJsonObject() && !Map.class.isAssignableFrom(field.getType())) {
                rejectUnknownKeys(value.getAsJsonObject(), field.getType(), prefix + key + ".");
            } else if (value.isJsonArray() && List.class.isAssignableFrom(field.getType())) {
                Class<?> elementType = (Class<?>) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
                if (elementType != String.class) {
                    int i = 0;
                    for (JsonElement element : value.getAsJsonArray()) {
                        if (element.isJsonObject()) {
                            rejectUnknownKeys(element.getAsJsonObject(), elementType, prefix + key + "[" + i + "].");
                        }
                        i++;
                    }
                }
            }
        }
    }

    // --- Validation ---

    private static void require(Object value, String key) {
        if (value == null) {
            throw new IllegalArgumentException("Missing required config key: '" + key + "'");
        }
    }

    private static void requirePositive(Number value, String key) {
        require(value, key);
        if (value.doubleValue() <= 0) {
            throw new IllegalArgumentException("Config key '" + key + "' must be > 0, got " + value);
        }
    }

    private static final Set<String> READ_LEVELS = Set.of(
            "EVENTUAL_LOCAL", "EVENTUAL_MAJORITY", "CAUSAL_LOCAL", "CAUSAL_MAJORITY", "LINEARIZABLE");

    private static void validateSla(Sla sla, String prefix, boolean isRead, int majority) {
        require(sla.slaId, prefix + "[].slaId");
        String slaPrefix = prefix + "[slaId=" + sla.slaId + "]";
        require(sla.rungs, slaPrefix + ".rungs");
        if (sla.rungs.isEmpty()) {
            throw new IllegalArgumentException(slaPrefix + ".rungs must not be empty");
        }
        for (SlaRung rung : sla.rungs) {
            if (isRead) {
                require(rung.level, slaPrefix + ".rungs[].level");
                if (rung.concern != null) {
                    throw new IllegalArgumentException(slaPrefix + " is a read SLA; rungs must not set 'concern'");
                }
                if (!READ_LEVELS.contains(rung.level)) {
                    throw new IllegalArgumentException(slaPrefix + ".rungs[].level '" + rung.level
                            + "' is unknown. Valid: " + READ_LEVELS);
                }
            } else {
                require(rung.concern, slaPrefix + ".rungs[].concern");
                if (rung.level != null) {
                    throw new IllegalArgumentException(slaPrefix + " is a write SLA; rungs must not set 'level'");
                }
                if (rung.concern < 1 || rung.concern > majority) {
                    throw new IllegalArgumentException(slaPrefix + ".rungs[].concern " + rung.concern
                            + " is out of range [1, " + majority + "]");
                }
            }
            requirePositive(rung.latencyMs, slaPrefix + ".rungs[].latencyMs");
            requirePositive(rung.profit, slaPrefix + ".rungs[].profit");
        }
    }

    private void validate() {
        require(cluster, "cluster");
        requirePositive(cluster.numServers, "cluster.numServers");
        require(cluster.serverHosts, "cluster.serverHosts");
        if (cluster.serverHosts.size() != cluster.numServers) {
            throw new IllegalArgumentException("cluster.serverHosts must have exactly cluster.numServers ("
                    + cluster.numServers + ") entries, got " + cluster.serverHosts.size());
        }
        requirePositive(cluster.serverBasePort, "cluster.serverBasePort");

        require(chameleon, "chameleon");
        requirePositive(chameleon.maxWaitMs, "chameleon.maxWaitMs");
        requirePositive(chameleon.clientLostTimeoutMs, "chameleon.clientLostTimeoutMs");
        requirePositive(chameleon.rttWindowSize, "chameleon.rttWindowSize");
        if (chameleon.rttWindowSize < 8) {
            throw new IllegalArgumentException("chameleon.rttWindowSize must be >= 8, got " + chameleon.rttWindowSize);
        }
        requirePositive(chameleon.clientRetryLimit, "chameleon.clientRetryLimit");
        requirePositive(chameleon.sMax, "chameleon.sMax");
        requirePositive(chameleon.controlIntervalMs, "chameleon.controlIntervalMs");
        requirePositive(chameleon.replicationBudgetPerSecond, "chameleon.replicationBudgetPerSecond");
        requirePositive(chameleon.uTarget, "chameleon.uTarget");
        requirePositive(chameleon.eta, "chameleon.eta");
        requirePositive(chameleon.lambdaMin, "chameleon.lambdaMin");

        require(slas, "slas");
        if (slas.isEmpty()) {
            throw new IllegalArgumentException("slas must register at least one application");
        }
        int majority = (cluster.numServers / 2) + 1;
        for (AppSlas app : slas) {
            require(app.applicationId, "slas[].applicationId");
            String appPrefix = "slas[applicationId=" + app.applicationId + "]";
            require(app.read, appPrefix + ".read");
            require(app.write, appPrefix + ".write");
            if (app.read.isEmpty() || app.write.isEmpty()) {
                throw new IllegalArgumentException(appPrefix + " must register at least one read and one write SLA");
            }
            for (Sla sla : app.read) {
                validateSla(sla, appPrefix + ".read", true, majority);
            }
            for (Sla sla : app.write) {
                validateSla(sla, appPrefix + ".write", false, majority);
            }
        }

        require(nodeFailure, "nodeFailure");
        require(nodeFailure.enabled, "nodeFailure.enabled");
        require(nodeFailure.failedNodeId, "nodeFailure.failedNodeId");
        if (nodeFailure.failedNodeId < 0 || nodeFailure.failedNodeId >= cluster.numServers) {
            throw new IllegalArgumentException("nodeFailure.failedNodeId must be in [0, " + (cluster.numServers - 1) + "]");
        }

        require(timedFailure, "timedFailure");
        require(timedFailure.enabled, "timedFailure.enabled");
        require(timedFailure.targetRole, "timedFailure.targetRole");
        if (!timedFailure.targetRole.equals("LEADER") && !timedFailure.targetRole.equals("FOLLOWER")) {
            throw new IllegalArgumentException("timedFailure.targetRole must be LEADER or FOLLOWER, got '" + timedFailure.targetRole + "'");
        }
        requirePositive(timedFailure.afterSeconds, "timedFailure.afterSeconds");

        require(geo, "geo");
        require(geo.enabled, "geo.enabled");
        requirePositive(geo.latencyMs, "geo.latencyMs");
        require(geo.scriptPath, "geo.scriptPath");
        requirePositive(geo.scriptTimeoutSeconds, "geo.scriptTimeoutSeconds");
        require(geo.clearOnExit, "geo.clearOnExit");
        require(geo.useSudo, "geo.useSudo");

        require(experiment, "experiment");
        require(experiment.bufferSeconds, "experiment.bufferSeconds");
        if (experiment.bufferSeconds < 0) {
            throw new IllegalArgumentException("experiment.bufferSeconds must be >= 0, got " + experiment.bufferSeconds);
        }
        require(experiment.runSinglePhase, "experiment.runSinglePhase");
        require(experiment.singlePhaseIndex, "experiment.singlePhaseIndex");
        if (experiment.runSinglePhase) {
            requirePositive(experiment.singlePhaseDurationSeconds, "experiment.singlePhaseDurationSeconds");
        }

        require(phases, "phases");
        if (phases.isEmpty()) {
            throw new IllegalArgumentException("phases must contain at least one phase");
        }
        for (int i = 0; i < phases.size(); i++) {
            PhaseConfig p = phases.get(i);
            String prefix = "phases[" + i + "]";
            require(p.name, prefix + ".name");
            requirePositive(p.durationSeconds, prefix + ".durationSeconds");
            requirePositive(p.totalTPS, prefix + ".totalTPS");
            require(p.readPercentage, prefix + ".readPercentage");
            require(p.writePercentage, prefix + ".writePercentage");
            if (Math.abs(p.readPercentage + p.writePercentage - 1.0) > 1e-6) {
                throw new IllegalArgumentException(prefix + ": readPercentage + writePercentage must sum to 1.0, got "
                        + (p.readPercentage + p.writePercentage));
            }
        }
        if (experiment.runSinglePhase && experiment.singlePhaseIndex >= phases.size()) {
            throw new IllegalArgumentException("experiment.singlePhaseIndex " + experiment.singlePhaseIndex
                    + " is out of range for " + phases.size() + " phases");
        }
    }
}
