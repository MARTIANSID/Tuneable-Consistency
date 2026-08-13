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
import org.example.Utility.KeySampler;
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
    private static final int TICK_MS = 100;

    private static KeySampler keySampler;

    // ===== Workload phases =====
    static class Phase {
        final String name;
        final int durationSeconds;
        final int totalTPS;
        final double readPercentage;
        final double writePercentage;

        Phase(String name, int durationSeconds, int totalTPS, double readPercentage, double writePercentage) {
            this.name = name;
            this.durationSeconds = durationSeconds;
            this.totalTPS = totalTPS;
            this.readPercentage = readPercentage;
            this.writePercentage = writePercentage;
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
    private static Map<Integer, List<Integer>> readSlaIdsByApp;
    private static Map<Integer, List<Integer>> writeSlaIdsByApp;
    private static Map<Integer, Map<Integer, Integer>> readSlaFloorsByApp;
    private static Map<Integer, Map<Integer, Integer>> writeSlaFloorsByApp;

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

        PHASES.clear();
        for (ExperimentConfig.PhaseConfig p : config.phases) {
            PHASES.add(new Phase(p.name, p.durationSeconds, p.totalTPS, p.readPercentage, p.writePercentage));
        }
        currentPhase = PHASES.get(0);

        // Per-application SLA id pools the workload draws from (uniformly),
        // and the per-SLA floor strengths the clients grade upgrades against.
        readSlaIdsByApp = new HashMap<>();
        writeSlaIdsByApp = new HashMap<>();
        readSlaFloorsByApp = new HashMap<>();
        writeSlaFloorsByApp = new HashMap<>();
        for (ExperimentConfig.AppSlas app : config.slas) {
            readSlaIdsByApp.put(app.applicationId, app.read.stream().map(s -> s.slaId).toList());
            writeSlaIdsByApp.put(app.applicationId, app.write.stream().map(s -> s.slaId).toList());
            Map<Integer, Integer> readFloors = new HashMap<>();
            for (ExperimentConfig.Sla sla : app.read) {
                readFloors.put(sla.slaId, sla.rungs.stream()
                        .mapToInt(r -> ReadLevel.valueOf(r.level).getNumber()).min().orElseThrow());
            }
            Map<Integer, Integer> writeFloors = new HashMap<>();
            for (ExperimentConfig.Sla sla : app.write) {
                writeFloors.put(sla.slaId, sla.rungs.stream().mapToInt(r -> r.concern).min().orElseThrow());
            }
            readSlaFloorsByApp.put(app.applicationId, readFloors);
            writeSlaFloorsByApp.put(app.applicationId, writeFloors);
        }

        keySampler = config.workload.keyDistribution.equals("zipfian")
                ? KeySampler.zipfian(config.workload.keySpace, config.workload.zipfianExponent)
                : KeySampler.uniform(config.workload.keySpace);

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
        MeasurementPlane.applyConfig(config);
        SlaRegistry.applyConfig(config);
        System.out.println("Loaded config from " + configPath.toAbsolutePath());

        clearCSVFiles();
        applyGeoSettingsIfEnabled();

        List<Server> servers = new ArrayList<>();
        serversImpl = new ArrayList<>();
        for (int i = 1; i <= NUM_OF_SERVERS; i++) {
            int port = SERVER_BASE_PORT + i;
            ServerImpl serverImpl = new ServerImpl(i - 1, NUM_OF_SERVERS);
            MeasurementPlane plane = new MeasurementPlane(i - 1, NUM_OF_SERVERS);
            Server server = ServerBuilder.forPort(port)
                    .addService(serverImpl)
                    .addService(new KvClientService(serverImpl, plane))
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
                    RTT_WINDOW_SIZE, CLIENT_RETRY_LIMIT, CLIENT_LOST_TIMEOUT_MS,
                    readSlaFloorsByApp.get(app + 1), writeSlaFloorsByApp.get(app + 1));
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

                    // The workload no longer picks consistency levels: each
                    // request names its application's SLA and the server
                    // decides. SLA ids are drawn uniformly per application;
                    // keys come from the configured distribution (uniform or
                    // zipfian), the same for reads and writes.
                    long now = System.currentTimeMillis();
                    for (int i = 0; i < writeCount; i++) {
                        int appId = appCursor + 1;
                        KvSessionClient client = appClients[appCursor];
                        appCursor = (appCursor + 1) % NUM_APPLICATIONS;
                        String key = "user" + keySampler.next(random);
                        client.sendWrite(key, "v-" + now + "-" + totalInjected.get(), pickSlaId(writeSlaIdsByApp, appId));
                        totalInjected.incrementAndGet();
                    }
                    for (int i = 0; i < readCount; i++) {
                        int appId = appCursor + 1;
                        KvSessionClient client = appClients[appCursor];
                        appCursor = (appCursor + 1) % NUM_APPLICATIONS;
                        client.sendRead("user" + keySampler.next(random), pickSlaId(readSlaIdsByApp, appId));
                        totalInjected.incrementAndGet();
                    }

                    long currentTime = System.currentTimeMillis();
                    if (currentTime - lastReportTime >= 3000) {
                        long elapsedSeconds = (currentTime - experimentStartTime) / 1000;
                        System.out.printf(
                                "[%02ds] Sent=%d | Responses=%d | Rejected=%d | Lost=%d | Violations=%d | Phase=%s | TotalTPS=%d%n",
                                elapsedSeconds, totalInjected.get(), ClientMetricsTracker.totalResponses(),
                                ClientMetricsTracker.totalRejected(), ClientMetricsTracker.totalLost(),
                                ClientMetricsTracker.totalViolations(), phase.name, phase.totalTPS);
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
            System.out.printf("%nExperiment completed. Sent=%d Responses=%d Rejected=%d Lost=%d SessionViolations=%d%n",
                    totalInjected.get(), ClientMetricsTracker.totalResponses(), ClientMetricsTracker.totalRejected(),
                    ClientMetricsTracker.totalLost(), violations);
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
            System.out.printf("   %d. %s: %ds | TotalTPS=%d | Read%%=%.0f%n",
                    PHASES.indexOf(p) + 1, p.name, p.durationSeconds, p.totalTPS, p.readPercentage * 100.0);
        }
        System.out.println();
    }

    private static int pickSlaId(Map<Integer, List<Integer>> slaIdsByApp, int appId) {
        List<Integer> ids = slaIdsByApp.get(appId);
        if (ids == null || ids.isEmpty()) {
            throw new IllegalStateException("No SLAs registered for applicationId " + appId);
        }
        return ids.size() == 1 ? ids.get(0) : ids.get(random.nextInt(ids.size()));
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

    /** Clear result CSVs at startup so every run starts from a clean slate. */
    private static void clearCSVFiles() {
        List<String> files = new ArrayList<>(List.of("client_metrics_global.csv"));
        for (int sid = 0; sid < 10; sid++) {
            files.add("occupancy_" + sid + ".csv");
            files.add("histograms_" + sid + ".csv");
        }
        for (String filename : files) {
            File file = new File(filename);
            if (file.exists() && !file.delete()) {
                System.out.println("Warning: could not delete " + filename);
            }
        }
    }
}
