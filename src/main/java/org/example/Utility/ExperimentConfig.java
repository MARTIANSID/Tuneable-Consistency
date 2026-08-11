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
    public Redis redis;
    public Consistency consistency;
    public NodeFailure nodeFailure;
    public TimedFailure timedFailure;
    public Geo geo;
    public Experiment experiment;
    public ServerTuning serverTuning;
    public BatchProcessorTuning batchProcessor;
    public TokenBucket tokenBucket;
    public List<PhaseConfig> phases;

    public static final class Cluster {
        public Integer numServers;
        public List<String> serverHosts;      // one entry per server
        public Integer serverBasePort;        // server i listens on basePort + i + 1
        public String clientCallbackHost;     // host servers dial back for ACKs
        public Integer callbackPortRangeStart; // first port tried for the callback server
        public Integer callbackPortRangeEnd;   // last port tried (inclusive)
    }

    public static final class Redis {
        public String host;
        public Integer port;
    }

    public static final class Consistency {
        public Boolean upgradeTransactions; // enable token-bucket based consistency upgrades
        public Boolean pressureMode;        // pressure-aware batch processing + deferrals
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
        public Boolean includeClientCallbackLatency;
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
        public Integer injectionBatchSize; // transactions per injected batch
    }

    public static final class ServerTuning {
        public Integer batchIntervalMs;   // batch processing cadence per server
        public Integer maxBatchSize;      // hard cap on transactions pulled per cycle
        public Integer maxItemsPerCycle;  // soft cap per cycle
    }

    public static final class BatchProcessorTuning {
        public Double leaderMaxLatencyMs;   // avg-latency cap on the leader
        public Double followerMaxLatencyMs; // avg-latency cap on followers
    }

    public static final class TokenBucket {
        public Double maxTokens;
        public Double refillRatePerSecond;
    }

    public static final class PhaseConfig {
        public String name;
        public Integer durationSeconds;
        public Integer totalTPS;
        public Double readPercentage;
        public Double writePercentage;
        public Map<String, Double> readDistribution;  // keys: EVENTUAL, CAUSAL_LOCAL, CAUSAL_MAJORITY, LINEARIZABLE
        public Map<String, Double> writeDistribution; // keys: write concern as string, "1".."majority"
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

    private static void requireDistribution(Map<String, Double> dist, String key) {
        require(dist, key);
        if (dist.isEmpty()) {
            throw new IllegalArgumentException("Config key '" + key + "' must not be empty");
        }
        double sum = 0;
        for (Map.Entry<String, Double> e : dist.entrySet()) {
            require(e.getValue(), key + "." + e.getKey());
            if (e.getValue() < 0) {
                throw new IllegalArgumentException("Config key '" + key + "." + e.getKey() + "' must be >= 0, got " + e.getValue());
            }
            sum += e.getValue();
        }
        if (Math.abs(sum - 1.0) > 1e-6) {
            throw new IllegalArgumentException("Config key '" + key + "' must sum to 1.0, got " + sum);
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
        require(cluster.clientCallbackHost, "cluster.clientCallbackHost");
        requirePositive(cluster.callbackPortRangeStart, "cluster.callbackPortRangeStart");
        requirePositive(cluster.callbackPortRangeEnd, "cluster.callbackPortRangeEnd");
        if (cluster.callbackPortRangeEnd < cluster.callbackPortRangeStart) {
            throw new IllegalArgumentException("cluster.callbackPortRangeEnd must be >= cluster.callbackPortRangeStart");
        }

        require(redis, "redis");
        require(redis.host, "redis.host");
        requirePositive(redis.port, "redis.port");

        require(consistency, "consistency");
        require(consistency.upgradeTransactions, "consistency.upgradeTransactions");
        require(consistency.pressureMode, "consistency.pressureMode");

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
        require(geo.includeClientCallbackLatency, "geo.includeClientCallbackLatency");
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
        requirePositive(experiment.injectionBatchSize, "experiment.injectionBatchSize");
        if (experiment.runSinglePhase) {
            requirePositive(experiment.singlePhaseDurationSeconds, "experiment.singlePhaseDurationSeconds");
        }

        require(serverTuning, "serverTuning");
        requirePositive(serverTuning.batchIntervalMs, "serverTuning.batchIntervalMs");
        requirePositive(serverTuning.maxBatchSize, "serverTuning.maxBatchSize");
        requirePositive(serverTuning.maxItemsPerCycle, "serverTuning.maxItemsPerCycle");

        require(batchProcessor, "batchProcessor");
        requirePositive(batchProcessor.leaderMaxLatencyMs, "batchProcessor.leaderMaxLatencyMs");
        requirePositive(batchProcessor.followerMaxLatencyMs, "batchProcessor.followerMaxLatencyMs");

        require(tokenBucket, "tokenBucket");
        requirePositive(tokenBucket.maxTokens, "tokenBucket.maxTokens");
        requirePositive(tokenBucket.refillRatePerSecond, "tokenBucket.refillRatePerSecond");

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
            requireDistribution(p.readDistribution, prefix + ".readDistribution");
            requireDistribution(p.writeDistribution, prefix + ".writeDistribution");
        }
        if (experiment.runSinglePhase && experiment.singlePhaseIndex >= phases.size()) {
            throw new IllegalArgumentException("experiment.singlePhaseIndex " + experiment.singlePhaseIndex
                    + " is out of range for " + phases.size() + " phases");
        }
    }
}
