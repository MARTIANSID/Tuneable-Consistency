package org.example.Server;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.example.Client.ClientMetricsTracker;
import org.example.Client.KvSessionClient;
import org.example.Utility.ExperimentConfig;
import org.example.Utility.ServerStatus.ServerCurrentStatus;
import org.example.raft.ReadLevel;

import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * Experiment entry point: boots the cluster (Raft service + client-facing KV
 * service per node), then drives the phased workload through per-application
 * KvSessionClients over real gRPC streams. All comparison metrics are
 * client-side (ClientMetricsTracker).
 */
public class Servers {

    // ===== Configuration (loaded from JSON at startup, see ExperimentConfig) =====
    public static int NUM_OF_SERVERS;
    private static int SERVER_BASE_PORT;
    private static List<String> SERVER_HOSTS;

    private static boolean ENABLE_NODE_NETWORK_FAILURE;
    private static int FAILED_NODE_ID;

    enum FailureTargetRole {
        LEADER,
        FOLLOWER
    }

    private static boolean ENABLE_TIMED_NODE_FAILURE;
    private static FailureTargetRole FAILURE_TARGET_ROLE;
    private static int FAILURE_AFTER_SECONDS;

    private static boolean ENABLE_GEO_SETTINGS;
    private static int GEO_LATENCY_MS;
    private static String GEO_SCRIPT_PATH;
    private static int GEO_SCRIPT_TIMEOUT_SECONDS;
    private static boolean CLEAR_GEO_SETTINGS_ON_EXIT;
    private static boolean USE_SUDO_FOR_GEO_SCRIPT;

    private static int RTT_WINDOW_SIZE;
    private static int CLIENT_RETRY_LIMIT;
    private static int CLIENT_LOST_TIMEOUT_MS;

    private static final int NUM_APPLICATIONS = 3;
    private static final int KEY_SPACE = 100;
    private static final int TICK_MS = 100;

    // ===== Workload phases =====
    static class Phase {
        final String name;
        final int durationSeconds;
        final int totalTPS;
        final double readPercentage;
        final double writePercentage;
        final Map<ReadLevel, Double> readDistribution;
        final Map<Integer, Double> writeDistribution;

        Phase(String name, int durationSeconds, int totalTPS, double readPercentage, double writePercentage,
                Map<ReadLevel, Double> readDistribution, Map<Integer, Double> writeDistribution) {
            this.name = name;
            this.durationSeconds = durationSeconds;
            this.totalTPS = totalTPS;
            this.readPercentage = readPercentage;
            this.writePercentage = writePercentage;
            this.readDistribution = readDistribution;
            this.writeDistribution = writeDistribution;
        }
    }

    private static final List<Phase> PHASES = new ArrayList<>();
    private static boolean RUN_SINGLE_PHASE;
    private static int SINGLE_PHASE_INDEX;
    private static final AtomicInteger currentPhaseIndex = new AtomicInteger(0);

    private static long TOTAL_EXPERIMENT_DURATION_MS;
    private static volatile long experimentStartTime;
    private static volatile boolean experimentRunning = true;
    private static volatile Phase currentPhase = null;
    private static volatile long phaseEndTime = 0;
    private static final Object phaseLock = new Object();

    private static final Random random = new Random();
    private static List<ServerImpl> serversImpl = new ArrayList<>();
    private static KvSessionClient[] appClients;

    static void applyConfig(ExperimentConfig config) {
        NUM_OF_SERVERS = config.cluster.numServers;
        SERVER_BASE_PORT = config.cluster.serverBasePort;
        SERVER_HOSTS = List.copyOf(config.cluster.serverHosts);

        ENABLE_NODE_NETWORK_FAILURE = config.nodeFailure.enabled;
        FAILED_NODE_ID = config.nodeFailure.failedNodeId;

        ENABLE_TIMED_NODE_FAILURE = config.timedFailure.enabled;
        FAILURE_TARGET_ROLE = FailureTargetRole.valueOf(config.timedFailure.targetRole);
        FAILURE_AFTER_SECONDS = config.timedFailure.afterSeconds;

        ENABLE_GEO_SETTINGS = config.geo.enabled;
        GEO_LATENCY_MS = config.geo.latencyMs;
        GEO_SCRIPT_PATH = config.geo.scriptPath;
        GEO_SCRIPT_TIMEOUT_SECONDS = config.geo.scriptTimeoutSeconds;
        CLEAR_GEO_SETTINGS_ON_EXIT = config.geo.clearOnExit;
        USE_SUDO_FOR_GEO_SCRIPT = config.geo.useSudo;

        RTT_WINDOW_SIZE = config.chameleon.rttWindowSize;
        CLIENT_RETRY_LIMIT = config.chameleon.clientRetryLimit;
        CLIENT_LOST_TIMEOUT_MS = config.chameleon.clientLostTimeoutMs;

        RUN_SINGLE_PHASE = config.experiment.runSinglePhase;
        SINGLE_PHASE_INDEX = config.experiment.singlePhaseIndex;

        int majority = (NUM_OF_SERVERS / 2) + 1;
        PHASES.clear();
        for (int i = 0; i < config.phases.size(); i++) {
            ExperimentConfig.PhaseConfig p = config.phases.get(i);
            Map<ReadLevel, Double> readDist = new HashMap<>();
            for (Map.Entry<String, Double> e : p.readDistribution.entrySet()) {
                try {
                    readDist.put(ReadLevel.valueOf(e.getKey()), e.getValue());
                } catch (IllegalArgumentException ex) {
                    throw new IllegalArgumentException("phases[" + i + "].readDistribution has unknown read level '"
                            + e.getKey() + "'. Valid: EVENTUAL_LOCAL, EVENTUAL_MAJORITY, CAUSAL_LOCAL, "
                            + "CAUSAL_MAJORITY, LINEARIZABLE");
                }
            }
            Map<Integer, Double> writeDist = new HashMap<>();
            for (Map.Entry<String, Double> e : p.writeDistribution.entrySet()) {
                int wc;
                try {
                    wc = Integer.parseInt(e.getKey());
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException("phases[" + i + "].writeDistribution key '" + e.getKey()
                            + "' is not an integer write concern");
                }
                if (wc < 1 || wc > majority) {
                    throw new IllegalArgumentException("phases[" + i + "].writeDistribution write concern " + wc
                            + " is out of range [1, " + majority + "] for " + NUM_OF_SERVERS + " servers");
                }
                writeDist.put(wc, e.getValue());
            }
            PHASES.add(new Phase(p.name, p.durationSeconds, p.totalTPS,
                    p.readPercentage, p.writePercentage, readDist, writeDist));
        }
        currentPhase = PHASES.get(0);

        long activeSeconds = RUN_SINGLE_PHASE
                ? config.experiment.singlePhaseDurationSeconds
                : PHASES.stream().mapToLong(p -> p.durationSeconds).sum();
        TOTAL_EXPERIMENT_DURATION_MS = (activeSeconds + config.experiment.bufferSeconds) * 1000L;
    }

    public static void main(String[] args) throws IOException {
        Path configPath = Path.of(args.length > 0 ? args[0] : "config.json");
        ExperimentConfig config = ExperimentConfig.load(configPath);
        applyConfig(config);
        ServerImpl.applyConfig(config);
        KvClientService.applyConfig(config);
        System.out.println("Loaded config from " + configPath.toAbsolutePath());

        clearCSVFiles();
        applyGeoSettingsIfEnabled();

        List<Server> servers = new ArrayList<>();
        serversImpl = new ArrayList<>();
        for (int i = 1; i <= NUM_OF_SERVERS; i++) {
            int port = SERVER_BASE_PORT + i;
            ServerImpl serverImpl = new ServerImpl(i - 1, NUM_OF_SERVERS);
            Server server = ServerBuilder.forPort(port)
                    .addService(serverImpl)
                    .addService(new KvClientService(serverImpl))
                    .build()
                    .start();
            System.out.println("Server" + (i - 1) + " started on port " + port);
            serverImpl.setUpStubs();
            servers.add(server);
            serversImpl.add(serverImpl);
        }

        applyNodeFailureConfig(serversImpl);

        appClients = new KvSessionClient[NUM_APPLICATIONS];
        for (int app = 0; app < NUM_APPLICATIONS; app++) {
            appClients[app] = new KvSessionClient(app + 1, SERVER_HOSTS, SERVER_BASE_PORT,
                    RTT_WINDOW_SIZE, CLIENT_RETRY_LIMIT, CLIENT_LOST_TIMEOUT_MS);
        }

        startPhasedInjection();

        for (Server server : servers) {
            try {
                server.awaitTermination();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // ===== Failure injection =====

    private static ServerImpl findLeader() {
        for (ServerImpl node : serversImpl) {
            if (node.status == ServerCurrentStatus.LEADER && !node.isDropAllServerNetworkTraffic()) {
                return node;
            }
        }
        return null;
    }

    private static ServerImpl waitForLeader(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            ServerImpl leader = findLeader();
            if (leader != null) {
                return leader;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return null;
    }

    private static void applyNodeFailureConfig(List<ServerImpl> nodes) {
        if (!ENABLE_NODE_NETWORK_FAILURE) {
            return;
        }
        if (FAILED_NODE_ID < 0 || FAILED_NODE_ID >= nodes.size()) {
            System.err.printf("Invalid FAILED_NODE_ID=%d (valid range: 0 to %d)%n",
                    FAILED_NODE_ID, Math.max(0, nodes.size() - 1));
            return;
        }
        nodes.get(FAILED_NODE_ID).setDropAllServerNetworkTraffic(true);
        System.out.printf("Simulated node failure enabled on server %d (inter-server RPCs are dropped)%n",
                FAILED_NODE_ID);
    }

    private static void scheduleTimedNodeFailure() {
        if (!ENABLE_TIMED_NODE_FAILURE) {
            return;
        }
        Thread failureScheduler = new Thread(() -> {
            long triggerAtMs = experimentStartTime + (Math.max(0, FAILURE_AFTER_SECONDS) * 1000L);
            while (experimentRunning && !Thread.currentThread().isInterrupted()) {
                if (System.currentTimeMillis() >= triggerAtMs) {
                    injectTimedFailure();
                    return;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "TimedFailureScheduler");
        failureScheduler.setDaemon(true);
        failureScheduler.start();
    }

    private static void injectTimedFailure() {
        ServerImpl leader = findLeader();
        ServerImpl target = null;

        if (FAILURE_TARGET_ROLE == FailureTargetRole.LEADER) {
            target = leader;
        } else {
            for (ServerImpl node : serversImpl) {
                if (node != leader) {
                    target = node;
                    break;
                }
            }
        }

        if (target == null) {
            System.err.printf("Timed failure skipped at t=%ds: could not resolve target=%s%n",
                    FAILURE_AFTER_SECONDS, FAILURE_TARGET_ROLE);
            return;
        }

        target.setDropAllServerNetworkTraffic(true);
        System.out.printf("Timed failure injected at t=%ds on %s node (serverId=%d)%n",
                FAILURE_AFTER_SECONDS, FAILURE_TARGET_ROLE, target.nodeId());
    }

    // ===== Geo latency (Linux tc/netem) =====

    private static void applyGeoSettingsIfEnabled() {
        if (!ENABLE_GEO_SETTINGS) {
            return;
        }
        if (!isRunningAsRoot() && !USE_SUDO_FOR_GEO_SCRIPT) {
            System.err.println("Geo settings enabled but current process is not root; skipping geo apply/clear.");
            return;
        }
        runGeoScript("apply", GEO_LATENCY_MS, NUM_OF_SERVERS);
        if (CLEAR_GEO_SETTINGS_ON_EXIT) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> runGeoScript("clear", 0, 0), "GeoSettingsCleanup"));
        }
    }

    private static void runGeoScript(String action, int latencyMs, int numServers) {
        List<String> command = new ArrayList<>();
        if (USE_SUDO_FOR_GEO_SCRIPT && !isRunningAsRoot()) {
            command.add("sudo");
            command.add("-n");
        }
        command.add("bash");
        command.add(GEO_SCRIPT_PATH);
        command.add(action);
        if ("apply".equals(action)) {
            command.add(String.valueOf(latencyMs));
            command.add(String.valueOf(numServers));
            command.add("none");
        }

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.redirectErrorStream(true);
        try {
            Process process = processBuilder.start();
            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append(System.lineSeparator());
                }
            }
            boolean finished = process.waitFor(GEO_SCRIPT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                System.err.printf("Geo script timed out (%s)%n", action);
                return;
            }
            if (process.exitValue() != 0) {
                System.err.printf("Geo script %s failed (exit=%d). Output:%n%s",
                        action, process.exitValue(), output);
            }
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            System.err.printf("Failed to run geo script action=%s : %s%n", action, e.getMessage());
        }
    }

    private static boolean isRunningAsRoot() {
        return "root".equals(System.getProperty("user.name"));
    }

    // ===== Phased workload =====

    private static void startPhasedInjection() {
        System.out.println("Waiting for leader election...");
        ServerImpl leader = waitForLeader(15_000);
        if (leader == null) {
            System.err.println("No leader elected, aborting injection");
            System.exit(1);
            return;
        }
        System.out.println("Leader elected: server " + leader.nodeId());

        System.out.println("Starting phased workload experiment...");
        printPhaseConfig();

        experimentStartTime = System.currentTimeMillis();
        experimentRunning = true;
        selectAndStartNewPhase();
        scheduleTimedNodeFailure();

        Thread phaseManager = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted() && experimentRunning) {
                try {
                    long currentTime = System.currentTimeMillis();
                    if (currentTime - experimentStartTime >= TOTAL_EXPERIMENT_DURATION_MS) {
                        experimentRunning = false;
                        break;
                    }
                    if (currentTime >= phaseEndTime && experimentRunning) {
                        Thread.sleep(500);
                        selectAndStartNewPhase();
                    }
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "PhaseManager");
        phaseManager.setDaemon(true);
        phaseManager.start();

        Thread systemInjector = new Thread(() -> {
            AtomicLong totalInjected = new AtomicLong(0);
            long lastReportTime = System.currentTimeMillis();
            int appCursor = 0;

            while (!Thread.currentThread().isInterrupted() && experimentRunning) {
                try {
                    Phase phase = currentPhase;
                    long tickStart = System.currentTimeMillis();
                    int perTick = Math.max(1, phase.totalTPS * TICK_MS / 1000);

                    int readCount = computeReadCount(perTick, phase);
                    int writeCount = Math.max(0, perTick - readCount);

                    List<ReadLevel> readChoices = buildDeterministicChoices(readCount,
                            phase.readDistribution, ReadLevel.EVENTUAL_LOCAL,
                            (a, b) -> a.name().compareTo(b.name()));
                    List<Integer> writeChoices = buildDeterministicChoices(writeCount,
                            phase.writeDistribution, 1, Integer::compareTo);

                    long now = System.currentTimeMillis();
                    for (int i = 0; i < writeCount; i++) {
                        KvSessionClient client = appClients[appCursor];
                        appCursor = (appCursor + 1) % NUM_APPLICATIONS;
                        String key = "user" + ((int) (totalInjected.get() % KEY_SPACE));
                        client.sendWrite(key, "v-" + now + "-" + totalInjected.get(), writeChoices.get(i));
                        totalInjected.incrementAndGet();
                    }
                    for (ReadLevel level : readChoices) {
                        KvSessionClient client = appClients[appCursor];
                        appCursor = (appCursor + 1) % NUM_APPLICATIONS;
                        client.sendRead("user" + random.nextInt(KEY_SPACE), level);
                        totalInjected.incrementAndGet();
                    }

                    long currentTime = System.currentTimeMillis();
                    if (currentTime - lastReportTime >= 3000) {
                        long elapsedSeconds = (currentTime - experimentStartTime) / 1000;
                        System.out.printf(
                                "[%02ds] Sent=%d | Responses=%d | Lost=%d | Violations=%d | Phase=%s | TotalTPS=%d%n",
                                elapsedSeconds, totalInjected.get(), ClientMetricsTracker.totalResponses(),
                                ClientMetricsTracker.totalLost(), ClientMetricsTracker.totalViolations(),
                                phase.name, phase.totalTPS);
                        lastReportTime = currentTime;
                    }

                    long elapsed = System.currentTimeMillis() - tickStart;
                    long sleepTime = TICK_MS - elapsed;
                    if (sleepTime > 0) {
                        Thread.sleep(sleepTime);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    System.err.println("System injection error: " + e.getMessage());
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }

            // Give in-flight requests a moment to resolve, then report.
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            ClientMetricsTracker.flushNow();
            long violations = ClientMetricsTracker.totalViolations();
            System.out.printf("%nExperiment completed. Sent=%d Responses=%d Lost=%d SessionViolations=%d%n",
                    totalInjected.get(), ClientMetricsTracker.totalResponses(), ClientMetricsTracker.totalLost(),
                    violations);
            for (KvSessionClient client : appClients) {
                client.close();
            }
            System.out.println("Shutting down servers...");
            System.exit(violations == 0 ? 0 : 2);
        }, "SystemInjector");
        systemInjector.setDaemon(true);
        systemInjector.start();
    }

    private static void selectAndStartNewPhase() {
        if (!experimentRunning) {
            return;
        }
        Phase newPhase = selectNextPhase();
        if (newPhase == null) {
            System.out.println("\nAll phases completed. Stopping experiment.");
            experimentRunning = false;
            return;
        }
        long newEndTime = RUN_SINGLE_PHASE
                ? experimentStartTime + TOTAL_EXPERIMENT_DURATION_MS
                : System.currentTimeMillis() + (newPhase.durationSeconds * 1000L);
        long experimentEndTime = experimentStartTime + TOTAL_EXPERIMENT_DURATION_MS;
        if (newEndTime > experimentEndTime) {
            newEndTime = experimentEndTime;
        }
        synchronized (phaseLock) {
            currentPhase = newPhase;
            phaseEndTime = newEndTime;
        }
        System.out.println("\n========================================");
        System.out.printf("Starting Phase %d/%d: %s%n", currentPhaseIndex.get(), PHASES.size(), newPhase.name);
        System.out.printf("   Duration: %ds | Total TPS: %d | Read%%: %.0f | Write%%: %.0f%n",
                newPhase.durationSeconds, newPhase.totalTPS,
                newPhase.readPercentage * 100.0, newPhase.writePercentage * 100.0);
        System.out.printf("   Reads: %s%n   Writes: %s%n", newPhase.readDistribution, newPhase.writeDistribution);
        System.out.println("========================================");
    }

    private static Phase selectNextPhase() {
        if (RUN_SINGLE_PHASE) {
            int clampedIndex = Math.max(0, Math.min(SINGLE_PHASE_INDEX, PHASES.size() - 1));
            return PHASES.get(clampedIndex);
        }
        int index = currentPhaseIndex.getAndIncrement();
        if (index >= PHASES.size()) {
            return null;
        }
        return PHASES.get(index);
    }

    private static void printPhaseConfig() {
        System.out.println("\nExperiment Configuration:");
        System.out.printf("   Total Duration: %d seconds%n", TOTAL_EXPERIMENT_DURATION_MS / 1000);
        System.out.println(RUN_SINGLE_PHASE ? "   Mode: SINGLE PHASE" : "   Mode: SEQUENTIAL PHASES");
        for (Phase p : PHASES) {
            System.out.printf("   %d. %s: %ds | TotalTPS=%d | Reads=%s | Writes=%s%n",
                    PHASES.indexOf(p) + 1, p.name, p.durationSeconds, p.totalTPS,
                    p.readDistribution, p.writeDistribution);
        }
        System.out.println();
    }

    private static int computeReadCount(int total, Phase phase) {
        if (total <= 0 || phase == null) {
            return 0;
        }
        double read = Math.max(0.0, phase.readPercentage);
        double write = Math.max(0.0, phase.writePercentage);
        double sum = read + write;
        if (sum <= 0.0) {
            return 0;
        }
        return (int) Math.round(total * (read / sum));
    }

    /**
     * Deterministic apportionment of a fixed count over a weighted
     * distribution (largest remainder method), so every tick reproduces the
     * configured mix exactly instead of sampling it.
     */
    private static <T> List<T> buildDeterministicChoices(int total, Map<T, Double> distribution, T fallbackKey,
            java.util.Comparator<T> keyOrder) {
        List<T> result = new ArrayList<>(Math.max(0, total));
        if (total <= 0 || distribution == null || distribution.isEmpty()) {
            return result;
        }
        double weightSum = distribution.values().stream().mapToDouble(v -> Math.max(0.0, v)).sum();
        if (weightSum <= 0.0) {
            return result;
        }

        Map<T, Integer> counts = new HashMap<>();
        Map<T, Double> fractions = new HashMap<>();
        int assigned = 0;
        for (Map.Entry<T, Double> entry : distribution.entrySet()) {
            double raw = Math.max(0.0, entry.getValue()) / weightSum * total;
            int base = (int) Math.floor(raw);
            counts.put(entry.getKey(), base);
            fractions.put(entry.getKey(), raw - base);
            assigned += base;
        }

        int remaining = total - assigned;
        List<T> keysByFraction = new ArrayList<>(distribution.keySet());
        keysByFraction.sort((a, b) -> {
            int cmp = Double.compare(fractions.getOrDefault(b, 0.0), fractions.getOrDefault(a, 0.0));
            return (cmp != 0) ? cmp : keyOrder.compare(a, b);
        });
        int idx = 0;
        while (remaining > 0 && !keysByFraction.isEmpty()) {
            T key = keysByFraction.get(idx % keysByFraction.size());
            counts.put(key, counts.getOrDefault(key, 0) + 1);
            remaining--;
            idx++;
        }

        List<T> sortedKeys = new ArrayList<>(counts.keySet());
        sortedKeys.sort(keyOrder);
        for (T key : sortedKeys) {
            for (int i = 0; i < counts.getOrDefault(key, 0); i++) {
                result.add(key);
            }
        }
        while (result.size() < total) {
            result.add(fallbackKey);
        }
        if (result.size() > total) {
            return new ArrayList<>(result.subList(0, total));
        }
        return result;
    }

    /** Clear result CSVs at startup so every run starts from a clean slate. */
    private static void clearCSVFiles() {
        String[] files = { "client_metrics_global.csv" };
        for (String filename : files) {
            File file = new File(filename);
            if (file.exists() && !file.delete()) {
                System.out.println("Warning: could not delete " + filename);
            }
        }
    }
}
