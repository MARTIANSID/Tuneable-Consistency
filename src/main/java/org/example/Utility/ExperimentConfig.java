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

    public String mode;
    public Cluster cluster;
    public ServerConfig server;
    public Chameleon chameleon;
    public ClientConfig client;
    public Workload workload;
    public List<AppSlas> slas;
    public NodeFailure nodeFailure;
    public TimedFailure timedFailure;
    public Geo geo;

    public static final class Cluster {
        public Integer numServers;
        public List<String> serverHosts; // one entry per server
        public Integer serverBasePort;   // server i listens on basePort + i + 1
    }

    /** Mode-independent server mechanics. */
    public static final class ServerConfig {
        public Integer maxWaitMs;          // clamp on every server-side wait; expiry falls back / acks what is there
        public Double sMax;                // occupancy slot budget; the hard cap rejects above 1.5x this in every mode
        public Double replicationBudgetPerSecond; // leader replication rate budget in entries/s (universal admission)
        public Boolean followerLinearizableReads; // followers serve LIN via a leader read-index round (all modes)
    }

    /** Chameleon economics; consulted only by the chameleon* modes. */
    public static final class Chameleon {
        public Integer controlIntervalMs;  // utilization / price-controller interval
        public Double uTarget;             // price controller utilization target (~0.85)
        public Double eta;                 // price controller gain (~1)
        public Double lambdaMin;           // price floor; only its order of magnitude matters
    }

    /** Mode-independent client mechanics. */
    public static final class ClientConfig {
        public Integer rttWindowSize;      // per-node sliding window of RTT samples (no-wait replies only)
        public Integer retryLimit;         // redirect/failure resends before giving up on a request
        public Integer lostTimeoutMs;      // no response within this -> the client scores the request as lost
        public Double explorationFraction; // fraction of reads routed randomly to keep per-node windows fresh (all routing options)
    }

    /** Key selection for the workload; reads and writes draw from the same distribution. */
    public static final class Workload {
        public Integer keySpace;        // number of distinct keys
        public String keyDistribution;  // "uniform" or "zipfian"
        public Double zipfianExponent;  // skew; weight of rank r is 1/(r+1)^exponent (used when zipfian)
        public Integer sessionsPerApplication; // independent session clients per app, drawn uniformly per request
        public Map<String, List<SlaShare>> mixes; // named weight vectors phases reference by key
        public List<PhaseConfig> phases;
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

    /**
     * Simulated geo latency, applied by run_all.sh (Linux tc/netem or macOS
     * dnctl/pfctt dummynet), not by the Java processes. When enabled,
     * cluster.serverHosts must be distinct literal loopback IPs (e.g.
     * 127.0.1.1..127.0.1.5, never 127.0.0.1, which identifies the client):
     * the (source IP, destination IP) pair is what the delay rules match on.
     */
    public static final class Geo {
        public Boolean enabled;
        // interServerLatencyMs[i][j]: one-way latency in ms added from server
        // i to server j (numServers x numServers; the diagonal must be 0).
        public Double[][] interServerLatencyMs;
        // clientToServerLatencyMs[i]: one-way latency in ms added between the
        // client and server i (numServers entries).
        public Double[] clientToServerLatencyMs;
    }

    /**
     * Phases control offered load and which named mix the traffic is drawn
     * from: each injected request is drawn from the mix by weight, which
     * determines the application, whether it is a read or a write, and the
     * SLA it names. The consistency-level mix is no longer configured
     * anywhere: the decision policy chooses levels from the registered SLAs.
     */
    public static final class PhaseConfig {
        public String name;
        public Integer durationSeconds;
        public Integer totalTPS;
        public String mix;      // key into workload.mixes
    }

    /** One slice of a phase's traffic: a registered SLA and its relative weight. */
    public static final class SlaShare {
        public Integer applicationId;
        public String type;      // read | write
        public Integer slaId;
        public Double weight;    // relative to the other entries; > 0
    }

    /** True when the server-side scorer resolves the subSLA target (chameleon* modes). */
    public boolean chameleonDecision() {
        return mode.equals("chameleon") || mode.equals("chameleonPileus");
    }

    /** True when the client routes with the Pileus windows. */
    public boolean pileusRouting() {
        return mode.equals("chameleonPileus") || mode.equals("pileus");
    }

    // --- Loading ---

    /**
     * Shell entry point for run_all.sh: fully load and validate the config,
     * then print the facts the orchestrator needs as key=value lines. It
     * doubles as a preflight (a bad config fails here, before any process
     * starts) and uses the same strict parser the Java processes use, so the
     * two can never disagree. durationSeconds mirrors the driver's total
     * experiment duration: active phase time plus the drain buffer.
     */
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: ExperimentConfig <config.yaml|config.json>");
            System.exit(64);
        }
        ExperimentConfig config = load(Path.of(args[0]));
        long durationSeconds = config.workload.phases.stream().mapToLong(p -> p.durationSeconds).sum();
        System.out.println("numServers=" + config.cluster.numServers);
        System.out.println("durationSeconds=" + durationSeconds);
        System.out.println("serverHosts=" + String.join(",", config.cluster.serverHosts));
        System.out.println("geoEnabled=" + config.geo.enabled);
        if (config.geo.enabled) {
            // Matrix rows are ';'-separated, entries ','-separated, in ms.
            StringBuilder matrix = new StringBuilder();
            for (int i = 0; i < config.geo.interServerLatencyMs.length; i++) {
                if (i > 0) {
                    matrix.append(';');
                }
                Double[] row = config.geo.interServerLatencyMs[i];
                for (int j = 0; j < row.length; j++) {
                    matrix.append(j > 0 ? "," : "").append(row[j]);
                }
            }
            System.out.println("geoInterServerLatencyMs=" + matrix);
            StringBuilder client = new StringBuilder();
            for (int i = 0; i < config.geo.clientToServerLatencyMs.length; i++) {
                client.append(i > 0 ? "," : "").append(config.geo.clientToServerLatencyMs[i]);
            }
            System.out.println("geoClientToServerLatencyMs=" + client);
        }
    }

    public static ExperimentConfig load(Path path) {
        String raw;
        try {
            raw = Files.readString(path);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot read config file: " + path.toAbsolutePath(), e);
        }

        String fileName = path.getFileName().toString().toLowerCase();
        JsonElement root = (fileName.endsWith(".yaml") || fileName.endsWith(".yml"))
                ? parseYaml(raw, path)
                : parseJson(raw, path);
        if (!root.isJsonObject()) {
            throw new IllegalArgumentException("Config root must be a mapping/object: " + path.toAbsolutePath());
        }

        rejectUnknownKeys(root.getAsJsonObject(), ExperimentConfig.class, "");

        ExperimentConfig config = new Gson().fromJson(root, ExperimentConfig.class);
        config.validate();
        return config;
    }

    private static JsonElement parseJson(String raw, Path path) {
        try {
            return JsonParser.parseString(raw);
        } catch (JsonSyntaxException e) {
            throw new IllegalArgumentException("Config file is not valid JSON: " + path.toAbsolutePath() + " - " + e.getMessage(), e);
        }
    }

    private static JsonElement parseYaml(String raw, Path path) {
        Object tree;
        try {
            tree = new org.yaml.snakeyaml.Yaml(new org.yaml.snakeyaml.constructor.SafeConstructor(
                    new org.yaml.snakeyaml.LoaderOptions())).load(raw);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Config file is not valid YAML: " + path.toAbsolutePath() + " - " + e.getMessage(), e);
        }
        if (tree == null) {
            throw new IllegalArgumentException("Config file is empty: " + path.toAbsolutePath());
        }
        return yamlToJsonTree(tree);
    }

    /** Convert a SnakeYAML tree into a Gson tree so both formats share the strict loader. */
    private static JsonElement yamlToJsonTree(Object node) {
        if (node == null) {
            return com.google.gson.JsonNull.INSTANCE;
        }
        if (node instanceof Map<?, ?> map) {
            JsonObject obj = new JsonObject();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                obj.add(String.valueOf(e.getKey()), yamlToJsonTree(e.getValue()));
            }
            return obj;
        }
        if (node instanceof List<?> list) {
            com.google.gson.JsonArray array = new com.google.gson.JsonArray();
            for (Object item : list) {
                array.add(yamlToJsonTree(item));
            }
            return array;
        }
        if (node instanceof Boolean bool) {
            return new com.google.gson.JsonPrimitive(bool);
        }
        if (node instanceof Number number) {
            return new com.google.gson.JsonPrimitive(number);
        }
        return new com.google.gson.JsonPrimitive(node.toString());
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
            if (value.isJsonObject() && Map.class.isAssignableFrom(field.getType())) {
                // Map keys are user-chosen names (e.g. mix names), so they are
                // not checked; each value is walked by the map's declared
                // value type so typos inside named entries are still rejected.
                java.lang.reflect.Type valueType = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[1];
                for (Map.Entry<String, JsonElement> entry : value.getAsJsonObject().entrySet()) {
                    String entryPrefix = prefix + key + "." + entry.getKey();
                    if (valueType instanceof ParameterizedType listType
                            && List.class.isAssignableFrom((Class<?>) listType.getRawType())
                            && entry.getValue().isJsonArray()) {
                        Class<?> elementType = (Class<?>) listType.getActualTypeArguments()[0];
                        int i = 0;
                        for (JsonElement element : entry.getValue().getAsJsonArray()) {
                            if (element.isJsonObject()) {
                                rejectUnknownKeys(element.getAsJsonObject(), elementType, entryPrefix + "[" + i + "].");
                            }
                            i++;
                        }
                    } else if (valueType instanceof Class<?> valueClass && entry.getValue().isJsonObject()) {
                        rejectUnknownKeys(entry.getValue().getAsJsonObject(), valueClass, entryPrefix + ".");
                    }
                }
            } else if (value.isJsonObject()) {
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

    private static final Set<String> KEY_DISTRIBUTIONS = Set.of("uniform", "zipfian");

    /**
     * The experiment arms. Each value bundles who resolves the subSLA target
     * with how contact servers are chosen:
     * chameleon        - server scorer decides, lowest-RTT routing
     * chameleonPileus  - server scorer decides, Pileus routing
     * pileus           - client picks (server, rung) by expected profit
     * highestProfit    - client targets the max-profit rung, lowest-RTT routing
     * lowestProfit     - client targets the floor rung, lowest-RTT routing
     */
    private static final Set<String> MODES = Set.of(
            "chameleon", "chameleonPileus", "pileus", "highestProfit", "lowestProfit");

    /** Keep in sync with ClientMetricsTracker.MAX_RUNGS (fixed CSV columns). */
    private static final int MAX_RUNGS_PER_SLA = 4;

    private static void validateSla(Sla sla, String prefix, boolean isRead, int majority) {
        require(sla.slaId, prefix + "[].slaId");
        String slaPrefix = prefix + "[slaId=" + sla.slaId + "]";
        require(sla.rungs, slaPrefix + ".rungs");
        if (sla.rungs.isEmpty()) {
            throw new IllegalArgumentException(slaPrefix + ".rungs must not be empty");
        }
        if (sla.rungs.size() > MAX_RUNGS_PER_SLA) {
            throw new IllegalArgumentException(slaPrefix + ".rungs must have at most " + MAX_RUNGS_PER_SLA
                    + " rungs (the client ledger's satisfied-rung columns are fixed), got " + sla.rungs.size());
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

        require(mode, "mode");
        if (!MODES.contains(mode)) {
            throw new IllegalArgumentException("mode must be one of " + MODES + ", got '" + mode + "'");
        }

        require(server, "server");
        requirePositive(server.maxWaitMs, "server.maxWaitMs");
        requirePositive(server.sMax, "server.sMax");
        requirePositive(server.replicationBudgetPerSecond, "server.replicationBudgetPerSecond");
        require(server.followerLinearizableReads, "server.followerLinearizableReads");

        require(chameleon, "chameleon");
        requirePositive(chameleon.controlIntervalMs, "chameleon.controlIntervalMs");
        requirePositive(chameleon.uTarget, "chameleon.uTarget");
        requirePositive(chameleon.eta, "chameleon.eta");
        requirePositive(chameleon.lambdaMin, "chameleon.lambdaMin");

        require(client, "client");
        requirePositive(client.rttWindowSize, "client.rttWindowSize");
        if (client.rttWindowSize < 8) {
            throw new IllegalArgumentException("client.rttWindowSize must be >= 8, got " + client.rttWindowSize);
        }
        requirePositive(client.retryLimit, "client.retryLimit");
        requirePositive(client.lostTimeoutMs, "client.lostTimeoutMs");
        require(client.explorationFraction, "client.explorationFraction");
        if (client.explorationFraction < 0 || client.explorationFraction >= 1) {
            throw new IllegalArgumentException("client.explorationFraction must be in [0, 1), got "
                    + client.explorationFraction);
        }

        require(workload, "workload");
        requirePositive(workload.keySpace, "workload.keySpace");
        require(workload.keyDistribution, "workload.keyDistribution");
        if (!KEY_DISTRIBUTIONS.contains(workload.keyDistribution)) {
            throw new IllegalArgumentException("workload.keyDistribution must be one of " + KEY_DISTRIBUTIONS
                    + ", got '" + workload.keyDistribution + "'");
        }
        if (workload.keyDistribution.equals("zipfian")) {
            requirePositive(workload.zipfianExponent, "workload.zipfianExponent");
        }
        requirePositive(workload.sessionsPerApplication, "workload.sessionsPerApplication");

        require(slas, "slas");
        if (slas.isEmpty()) {
            throw new IllegalArgumentException("slas must register at least one application");
        }
        // The workload driver indexes session pools by applicationId - 1, so
        // ids must be exactly 1..N.
        java.util.Set<Integer> appIds = new java.util.HashSet<>();
        for (AppSlas app : slas) {
            require(app.applicationId, "slas[].applicationId");
            appIds.add(app.applicationId);
        }
        for (int expected = 1; expected <= slas.size(); expected++) {
            if (!appIds.contains(expected)) {
                throw new IllegalArgumentException("slas applicationIds must be exactly 1.." + slas.size()
                        + " (distinct, contiguous), got " + new java.util.TreeSet<>(appIds));
            }
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
        require(geo.interServerLatencyMs, "geo.interServerLatencyMs");
        if (geo.interServerLatencyMs.length != cluster.numServers) {
            throw new IllegalArgumentException("geo.interServerLatencyMs must have " + cluster.numServers
                    + " rows (one per server), got " + geo.interServerLatencyMs.length);
        }
        for (int i = 0; i < geo.interServerLatencyMs.length; i++) {
            Double[] row = geo.interServerLatencyMs[i];
            require(row, "geo.interServerLatencyMs[" + i + "]");
            if (row.length != cluster.numServers) {
                throw new IllegalArgumentException("geo.interServerLatencyMs[" + i + "] must have "
                        + cluster.numServers + " entries (one per server), got " + row.length);
            }
            for (int j = 0; j < row.length; j++) {
                require(row[j], "geo.interServerLatencyMs[" + i + "][" + j + "]");
                if (!(row[j] >= 0) || !Double.isFinite(row[j])) {
                    throw new IllegalArgumentException("geo.interServerLatencyMs[" + i + "][" + j
                            + "] must be a non-negative finite number, got " + row[j]);
                }
                if (i == j && row[j] != 0) {
                    throw new IllegalArgumentException("geo.interServerLatencyMs[" + i + "][" + j
                            + "] is a server's latency to itself and must be 0, got " + row[j]);
                }
            }
        }
        require(geo.clientToServerLatencyMs, "geo.clientToServerLatencyMs");
        if (geo.clientToServerLatencyMs.length != cluster.numServers) {
            throw new IllegalArgumentException("geo.clientToServerLatencyMs must have " + cluster.numServers
                    + " entries (one per server), got " + geo.clientToServerLatencyMs.length);
        }
        for (int i = 0; i < geo.clientToServerLatencyMs.length; i++) {
            Double v = geo.clientToServerLatencyMs[i];
            require(v, "geo.clientToServerLatencyMs[" + i + "]");
            if (!(v >= 0) || !Double.isFinite(v)) {
                throw new IllegalArgumentException("geo.clientToServerLatencyMs[" + i
                        + "] must be a non-negative finite number, got " + v);
            }
        }
        if (geo.enabled) {
            // The delay rules match on (source IP, destination IP), so every
            // server needs its own literal loopback IP, and 127.0.0.1 is
            // reserved as the client's identity (unbound sockets use it).
            java.util.Set<String> distinctHosts = new java.util.HashSet<>();
            for (int i = 0; i < cluster.serverHosts.size(); i++) {
                String host = cluster.serverHosts.get(i);
                if (!host.matches("\\d+\\.\\d+\\.\\d+\\.\\d+")) {
                    throw new IllegalArgumentException("geo.enabled requires cluster.serverHosts[" + i
                            + "] to be a literal IPv4 address (e.g. 127.0.1." + (i + 1) + "), got '" + host + "'");
                }
                if (host.equals("127.0.0.1")) {
                    throw new IllegalArgumentException("geo.enabled forbids cluster.serverHosts[" + i
                            + "] = 127.0.0.1: that address identifies the client in the delay rules");
                }
                if (!distinctHosts.add(host)) {
                    throw new IllegalArgumentException("geo.enabled requires distinct cluster.serverHosts; '"
                            + host + "' appears more than once");
                }
            }
        }

        // Mixes and phases live under workload but are validated here, after
        // slas, because every mix entry must name a registered SLA.
        java.util.Set<String> registeredSlas = new java.util.HashSet<>();
        for (AppSlas app : slas) {
            for (Sla sla : app.read) {
                registeredSlas.add(app.applicationId + "/read/" + sla.slaId);
            }
            for (Sla sla : app.write) {
                registeredSlas.add(app.applicationId + "/write/" + sla.slaId);
            }
        }
        require(workload.mixes, "workload.mixes");
        if (workload.mixes.isEmpty()) {
            throw new IllegalArgumentException("workload.mixes must define at least one mix");
        }
        for (Map.Entry<String, List<SlaShare>> mix : workload.mixes.entrySet()) {
            String mixPrefix = "workload.mixes." + mix.getKey();
            require(mix.getValue(), mixPrefix);
            if (mix.getValue().isEmpty()) {
                throw new IllegalArgumentException(mixPrefix + " must contain at least one entry");
            }
            for (int j = 0; j < mix.getValue().size(); j++) {
                SlaShare share = mix.getValue().get(j);
                String sharePrefix = mixPrefix + "[" + j + "]";
                require(share.applicationId, sharePrefix + ".applicationId");
                require(share.type, sharePrefix + ".type");
                require(share.slaId, sharePrefix + ".slaId");
                require(share.weight, sharePrefix + ".weight");
                if (!share.type.equals("read") && !share.type.equals("write")) {
                    throw new IllegalArgumentException(sharePrefix + ".type must be read or write, got '"
                            + share.type + "'");
                }
                if (!(share.weight > 0) || !Double.isFinite(share.weight)) {
                    throw new IllegalArgumentException(sharePrefix + ".weight must be a positive finite number, got "
                            + share.weight);
                }
                String key = share.applicationId + "/" + share.type + "/" + share.slaId;
                if (!registeredSlas.contains(key)) {
                    throw new IllegalArgumentException(sharePrefix + " references unregistered SLA " + key
                            + " (registered: " + new java.util.TreeSet<>(registeredSlas) + ")");
                }
            }
        }
        List<PhaseConfig> phases = workload.phases;
        require(phases, "workload.phases");
        if (phases.isEmpty()) {
            throw new IllegalArgumentException("workload.phases must contain at least one phase");
        }
        for (int i = 0; i < phases.size(); i++) {
            PhaseConfig p = phases.get(i);
            String prefix = "workload.phases[" + i + "]";
            require(p.name, prefix + ".name");
            requirePositive(p.durationSeconds, prefix + ".durationSeconds");
            requirePositive(p.totalTPS, prefix + ".totalTPS");
            require(p.mix, prefix + ".mix");
            if (!workload.mixes.containsKey(p.mix)) {
                throw new IllegalArgumentException(prefix + ".mix '" + p.mix + "' is not defined in workload.mixes "
                        + new java.util.TreeSet<>(workload.mixes.keySet()));
            }
        }
    }
}
