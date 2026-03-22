package org.example.Server;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.example.Client.ClientServerImpl;
import org.ds.paxos.Ack;
import org.ds.paxos.ClientMessage;
import org.ds.paxos.ReadConcern;
import org.ds.paxos.ReadLevel;
import org.ds.paxos.Transaction;
import org.ds.paxos.TimeStampProto;
import org.example.Utility.TransactionOption;

public class Servers{

    // set the number of servers from here
    public static final int NUM_OF_SERVERS = 3;

    // Toggle transaction upgrading (token-bucket based consistency tuning).
    // false => execute batch at original consistency, no upgrades/deferrals.
    private static final boolean UPGRADE_TRANSACTIONS = false;
    
    // Static reference to TransactionInjector for testing
    public static TransactionInjector injector;
    // Tracks latest committed timestamp from ACKs sent by servers to client callback
    private static ClientServerImpl clientServerImpl;
    private static Server clientCallbackServer;
    private static int clientCallbackPort = 9000;

    // ========== Workload Phases (similar to StressTest.java) ==========
    enum ReadClass {
        LINEARIZABLE,
        CAUSAL_LOCAL,
        CAUSAL_MAJORITY,
        EVENTUAL
    }

    static class Phase {
        String name;
        int durationSeconds;
        int totalTPS;
        Map<ReadClass, Double> readDistribution; // system-wide read shares by class
        Map<Integer, Double> writeDistribution; // system-wide write shares by writeConcern

        Phase(String name,
              int durationSeconds,
              int totalTPS,
              Map<ReadClass, Double> readDistribution,
              Map<Integer, Double> writeDistribution) {
            this.name = name;
            this.durationSeconds = durationSeconds;
            this.totalTPS = totalTPS;
            this.readDistribution = readDistribution;
            this.writeDistribution = writeDistribution;
        }
    }

    private static final List<Phase> PHASES = new ArrayList<>();
    static {
        // light workload: read-heavy mix (90% reads, 10% writes)
        PHASES.add(new Phase(
            "Light", 50, 10000,
            Map.of(
                ReadClass.EVENTUAL, 0.70,
                ReadClass.CAUSAL_LOCAL, 0.12,
                ReadClass.CAUSAL_MAJORITY, 0.06,
                ReadClass.LINEARIZABLE, 0.02
            ),
            Map.of(1, 0.08, 2, 0.02)
        ));
        // heavy workload: more strict writes and stricter reads
        PHASES.add(new Phase(
            "Heavy", 50, 10000,
            Map.of(
                ReadClass.EVENTUAL, 0.50,
                ReadClass.CAUSAL_LOCAL, 0.12,
                ReadClass.CAUSAL_MAJORITY, 0.08,
                ReadClass.LINEARIZABLE, 0.05
            ),
            Map.of(1, 0.10, 2, 0.15)
        ));
        // medium workload: read-heavy but less skewed than Light
        PHASES.add(new Phase(
            "Medium", 50, 10000,
            Map.of(
                ReadClass.EVENTUAL, 0.58,
                ReadClass.CAUSAL_LOCAL, 0.12,
                ReadClass.CAUSAL_MAJORITY, 0.10,
                ReadClass.LINEARIZABLE, 0.05
            ),
            Map.of(1, 0.10, 2, 0.05)
        ));
    }

    // Phase execution mode
    // true: run only one selected phase for the whole experiment
    // false: cycle through all phases sequentially
    private static final boolean RUN_SINGLE_PHASE = false;
    // 0-based phase index used when RUN_SINGLE_PHASE=true
    private static final int SINGLE_PHASE_INDEX = 0;
    
    // Track current phase index for sequential execution
    private static final AtomicInteger currentPhaseIndex = new AtomicInteger(0);

    private static final int BATCH_SIZE = 1000;  // Transactions per batch (increased to reduce gRPC overhead)
    private static final Random random = new Random();

    // Read transaction configuration is phase-driven via system-wide phase distributions.
    // Track last write timestamp for causal/linearizable reads
    private static volatile TimeStampProto lastWriteTimestamp = TimeStampProto.newBuilder()
            .setP(System.currentTimeMillis()).setL(0).build();
    
    // Experiment parameters
    private static final long TOTAL_EXPERIMENT_DURATION_MS = 150000;  // 150 seconds total
    private static volatile long experimentStartTime;
    private static volatile boolean experimentRunning = true;
    
    // Current phase parameters (volatile for thread-safe reads)
    private static volatile Phase currentPhase = PHASES.get(0);  // Start with Light (sequential phases override this)
    private static volatile long phaseEndTime = 0;
    private static final Object phaseLock = new Object();
    
    // Incoming transaction tracking
    private static final Queue<Long> incomingTransactionTimestamps = new LinkedList<>();
    private static final Object incomingTransactionLock = new Object();
    private static final AtomicLong lastPrintTime = new AtomicLong(0);
    private static final AtomicInteger anyServerReadCursor = new AtomicInteger(0);
    private static final AtomicInteger followerReadCursor = new AtomicInteger(0);

    public static void main(String[] args) throws IOException{
        // Clear all CSV files at startup
        clearCSVFiles();

        // Set whether servers should upgrade/tune transaction consistency.
        ServerImpl.setUpgradeTransactionsEnabled(UPGRADE_TRANSACTIONS);

        // Start client callback endpoint so ACK timestamps can be consumed.
        clientServerImpl = new ClientServerImpl();
        clientCallbackPort = findAvailablePort(9000, 9100);
        clientCallbackServer = ServerBuilder.forPort(clientCallbackPort)
            .addService(clientServerImpl)
            .build()
            .start();
        clientServerImpl.setUpStubs();
        System.out.println("Client callback server started on port " + clientCallbackPort);
        
        List<Server> servers = new ArrayList<>();
        List<ServerImpl> serversImpl = new ArrayList<>();
        for (int i = 1; i <= NUM_OF_SERVERS; i++) {
            int port = 8000 + i; // Ports 8000 to 8004
            ServerImpl serverImpl = new ServerImpl(i - 1, NUM_OF_SERVERS);
            Server server = ServerBuilder.forPort(port)
                    .addService(serverImpl)
//                    .executor(Executors.newFixedThreadPool(1)) // limit to 2 threads
                    .build()
                    .start();
            System.out.println("Server" + (i - 1) + " started on port " + port);
            serverImpl.setUpStubs();
            servers.add(server);
            serversImpl.add(serverImpl);
        }
        
        // Initialize the TransactionInjector
        injector = new TransactionInjector(serversImpl);
        
        // Start transaction injection in a separate thread
        startPhasedInjection();
        
        for (int i = 0; i < servers.size(); i++) {
            final int index = i;
            Server server = servers.get(i);
                try {
                    server.awaitTermination();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
        }
    }

    /**
     * Starts phased transaction injection with:
     * 1. Phase Manager - changes phase parameters periodically
     * 2. System Injector - injects phase.totalTPS across the whole cluster
     */
    private static void startPhasedInjection() {
        System.out.println("⏳ Waiting for leader election...");
        ServerImpl leader = injector.waitForLeader(10000);
        if (leader == null) {
            System.err.println("❌ No leader elected, aborting injection");
            return;
        }

        System.out.println("🚀 Starting phased workload experiment...");
        printPhaseConfig();
        
        // Initialize experiment
        experimentStartTime = System.currentTimeMillis();
        experimentRunning = true;
        selectAndStartNewPhase();
        
        // Thread 1: Phase Manager - switches phases and monitors total experiment time
        Thread phaseManager = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted() && experimentRunning) {
                try {
                    // Check if total experiment has ended
                    long currentTime = System.currentTimeMillis();
                    if (currentTime - experimentStartTime >= TOTAL_EXPERIMENT_DURATION_MS) {
                        System.out.println("\n🏁 Experiment completed!");
                        experimentRunning = false;
                        break;
                    }
                    
                    // Check if current phase has ended (but experiment still running)
                    if (currentTime >= phaseEndTime && experimentRunning) {
                        // Small gap between phases for clean transition logging
                        Thread.sleep(500);
                        selectAndStartNewPhase();
                    }
                    Thread.sleep(100);  // Check every 100ms
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "PhaseManager");
        phaseManager.setDaemon(true);
        phaseManager.start();
        
        // Thread 2: System Injector — totalTPS distributed using phase read/write shares.
        Thread systemInjector = new Thread(() -> {
            AtomicInteger totalInjected = new AtomicInteger(0);
            long lastReportTime = System.currentTimeMillis();

            while (!Thread.currentThread().isInterrupted() && experimentRunning) {
                try {
                    if (!experimentRunning) break;

                    Phase phase = currentPhase;

                    int batchesPerSecond = Math.max(1, phase.totalTPS / BATCH_SIZE);
                    long intervalMs = 1000 / batchesPerSecond;
                    int actualBatchSize = Math.max(1, phase.totalTPS / batchesPerSecond);

                    long batchStart = System.currentTimeMillis();

                    ServerImpl currentLeader = injector.getLeader();
                    if (currentLeader != null) {
                        int injected = injectSystemBatch(actualBatchSize, phase, currentLeader);
                        totalInjected.addAndGet(injected);
                    }

                    // Report every 3 seconds
                    long currentTime = System.currentTimeMillis();
                    long elapsedSeconds = (currentTime - experimentStartTime) / 1000;
                    if (currentTime - lastReportTime >= 3000) {
                        System.out.printf("📊 [%02ds] Injected=%d | Phase=%s | TotalTPS=%d%n",
                                elapsedSeconds, totalInjected.get(), phase.name, phase.totalTPS);
                        lastReportTime = currentTime;
                    }

                    long elapsed = System.currentTimeMillis() - batchStart;
                    long sleepTime = intervalMs - elapsed;
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

            System.out.printf("\n🏆 Experiment completed! Injected: %d transactions%n",
                    totalInjected.get());
            System.out.println("\n✅ Shutting down servers...");
            System.exit(0);
        }, "SystemInjector");
        systemInjector.setDaemon(true);
        systemInjector.start();
    }
    
    /**
     * Select a new phase and update the shared phase parameters atomically
     * Phases run SEQUENTIALLY to guarantee all are experienced
     */
    private static void selectAndStartNewPhase() {
        if (!experimentRunning) return;
        
        Phase newPhase = selectNextPhase();
        if (newPhase == null) {
            System.out.println("\n🏁 All phases completed (Phase 1 -> Phase 2 -> Phase 3). Stopping experiment.");
            experimentRunning = false;
            return;
        }
        long newEndTime;
        if (RUN_SINGLE_PHASE) {
            newEndTime = experimentStartTime + TOTAL_EXPERIMENT_DURATION_MS;
        } else {
            newEndTime = System.currentTimeMillis() + (newPhase.durationSeconds * 1000L);
        }
        
        // Don't let phase extend beyond total experiment duration
        long experimentEndTime = experimentStartTime + TOTAL_EXPERIMENT_DURATION_MS;
        if (newEndTime > experimentEndTime) {
            newEndTime = experimentEndTime;
        }
        
        synchronized (phaseLock) {
            currentPhase = newPhase;
            phaseEndTime = newEndTime;
        }
        
        long remainingSeconds = (experimentEndTime - System.currentTimeMillis()) / 1000;
        int phaseNum = currentPhaseIndex.get();
        System.out.println("\n========================================");
        System.out.printf("📌 Starting Phase %d/%d: %s (Experiment time remaining: %ds)%n", 
                phaseNum, PHASES.size(), newPhase.name, remainingSeconds);
        System.out.printf("   Duration: %ds | Total TPS: %d%n", newPhase.durationSeconds, newPhase.totalTPS);
        System.out.printf("   Read Distribution: %s%n", newPhase.readDistribution);
        System.out.printf("   Write Distribution: %s%n", newPhase.writeDistribution);
        System.out.println("========================================");
    }
    
    /**
     * Robust batch injection - tries multiple servers if needed
     */
    private static boolean injectBatchRobust(List<ClientMessage> batch) {
        // Convert ClientMessage list to TransactionOption list
        List<TransactionOption> transactionOptions = batch.stream()
                .map(TransactionOption::fromClientMessage)
                .collect(Collectors.toList());
        
        // Track incoming transactions BEFORE injection
        trackIncomingTransactions(batch.size());
        
        // First try: current leader
        ServerImpl leader = injector.getLeader();
        if (leader != null) {
            leader.batchLock.lock();
            try {
                leader.batchOfTransactions.addAll(transactionOptions);
                return true;
            } finally {
                leader.batchLock.unlock();
            }
        }
        
        // Second try: any server (they will forward to leader or queue)
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            ServerImpl server = injector.getServerById(i);
            if (server != null) {
                server.batchLock.lock();
                try {
                    server.batchOfTransactions.addAll(transactionOptions);
                    return true;
                } finally {
                    server.batchLock.unlock();
                }
            }
        }
    
        return false;
    }
    
    /**
     * Tracks incoming transactions and logs the rate to CSV
     */
    private static void trackIncomingTransactions(int count) {
        long currentTime = System.currentTimeMillis();
        
        synchronized (incomingTransactionLock) {
            // Add timestamps for all transactions in this batch
            for (int i = 0; i < count; i++) {
                incomingTransactionTimestamps.add(currentTime);
            }
            
            // Remove timestamps older than 1 second
            while (!incomingTransactionTimestamps.isEmpty()
                    && currentTime - incomingTransactionTimestamps.peek() >= 1000L) {
                incomingTransactionTimestamps.poll();
            }
            
            // Print and log to CSV every second
            long lastPrint = lastPrintTime.get();
            if (currentTime - lastPrint >= 1000L && lastPrintTime.compareAndSet(lastPrint, currentTime)) {
                int incomingTPS = incomingTransactionTimestamps.size();
                System.out.printf("📥 [Incoming Transactions] TPS: %d | Time: %s%n",
                        incomingTPS,
                        new java.text.SimpleDateFormat("HH:mm:ss").format(new java.util.Date(currentTime)));
                
                // Log to CSV file
                try {
                    File file = new File("incoming_transaction_rate_global.csv");
                    boolean writeHeader = !file.exists() || file.length() == 0;
                    try (FileWriter fw = new FileWriter(file, true); PrintWriter out = new PrintWriter(fw)) {
                        if (writeHeader) {
                            out.println("Timestamp,IncomingTransactionCount,IncomingTPS");
                        }
                        out.printf("%d,%d,%d%n", currentTime, incomingTPS, incomingTPS);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Select the next phase sequentially (cycles through all phases)
     */
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

    /**
     * Print phase configuration for the phased experiment.
     */
    private static void printPhaseConfig() {
        System.out.println("\n📊 Experiment Configuration:");
        System.out.printf("   Total Duration: %d seconds%n", TOTAL_EXPERIMENT_DURATION_MS / 1000);
        if (RUN_SINGLE_PHASE) {
            System.out.println("   Mode: SINGLE PHASE");
        } else {
            System.out.println("   Mode: SEQUENTIAL PHASES");
        }
        System.out.println("   Phases:");
        int totalDuration = 0;
        for (Phase p : PHASES) {
            System.out.printf("   %d. %s: %ds | TotalTPS=%d | Reads=%s | Writes=%s%n",
                PHASES.indexOf(p) + 1,
                p.name,
                p.durationSeconds,
                p.totalTPS,
                p.readDistribution,
                p.writeDistribution);
            totalDuration += p.durationSeconds;
        }
        System.out.printf("   Total phase duration: %ds (buffer: %ds)%n", totalDuration, 
                (int)(TOTAL_EXPERIMENT_DURATION_MS/1000) - totalDuration);
        System.out.println();
    }

    private static int injectSystemBatch(int size, Phase phase, ServerImpl currentLeader) {
        int leaderId = findLeaderId(currentLeader);
        if (leaderId < 0) {
            return 0;
        }

        Map<ServerImpl, List<TransactionOption>> perServerBatch = new HashMap<>();

        int readCount = computeOperationCount(size, phase.readDistribution, phase.writeDistribution);
        int writeCount = Math.max(0, size - readCount);

        Map<ReadClass, Double> normalizedReadDistribution = normalizeDistribution(phase.readDistribution);
        Map<Integer, Double> normalizedWriteDistribution = normalizeDistribution(phase.writeDistribution);

        List<ReadClass> readChoices = buildDeterministicReadChoices(
                readCount,
                normalizedReadDistribution,
                ReadClass.EVENTUAL);
        List<Integer> writeChoices = buildDeterministicChoices(
                writeCount,
                normalizedWriteDistribution,
                1);

        long now = System.currentTimeMillis();

        for (int i = 0; i < writeCount; i++) {
            int minConsistency = writeChoices.get(i);
            ClientMessage writeMessage = generateWriteTransaction(now, i, minConsistency);
            perServerBatch
                    .computeIfAbsent(currentLeader, ignored -> new ArrayList<>())
                    .add(TransactionOption.fromClientMessage(writeMessage));
        }

        TimeStampProto readAnchorTs = getLatestAckedTimestamp();
        for (ReadClass readClass : readChoices) {
            int targetServerId = selectReadTargetServerId(readClass, leaderId);
            ServerImpl targetServer = injector.getServerById(targetServerId);
            if (targetServer == null) {
                continue;
            }
            ClientMessage readMessage = generateReadTransaction(readAnchorTs, readClass);
            perServerBatch
                    .computeIfAbsent(targetServer, ignored -> new ArrayList<>())
                    .add(TransactionOption.fromClientMessage(readMessage));
        }

        int injected = 0;
        for (Map.Entry<ServerImpl, List<TransactionOption>> entry : perServerBatch.entrySet()) {
            ServerImpl server = entry.getKey();
            List<TransactionOption> txs = entry.getValue();
            if (txs == null || txs.isEmpty()) {
                continue;
            }
            server.batchLock.lock();
            try {
                server.batchOfTransactions.addAll(txs);
                injected += txs.size();
            } finally {
                server.batchLock.unlock();
            }
        }

        if (injected > 0) {
            trackIncomingTransactions(injected);
        }

        return injected;
    }

    private static ClientMessage generateWriteTransaction(long now, int sequence, int minConsistency) {
        String txId = UUID.randomUUID().toString();
        String sender = "user" + (sequence % 100);
        String receiver = "user" + ((sequence + 50) % 100);
        double amount = 1.0 + (sequence % 10);

        int applicationId = 1 + random.nextInt(3);

        Transaction transaction = Transaction.newBuilder()
                .setId(txId)
                .setSender(sender)
                .setReceiver(receiver)
                .setAmount(amount)
                .setTransactionSendTimeInMs(now)
                .setMinRequiredConsistency(minConsistency)
                .setApplicationId(applicationId)
                .setBaseProfit((double) applicationId)
                .setExtraProfitMajority((double) applicationId)
                .setExtraIntermediateProfit((double) applicationId)
                .setWriteConcern(minConsistency)
                .setIsReadOnly(false)
                .build();

        TimeStampProto ts = TimeStampProto.newBuilder()
                .setP(now)
                .setL(sequence)
                .build();

        lastWriteTimestamp = ts;

        return ClientMessage.newBuilder()
                .setT(transaction)
                .setWriteConcern(minConsistency)
                .setTimeStamp(ts)
                .setCallbackHost("localhost")
                .setCallbackPort(clientCallbackPort)
                .build();
    }

    /**
     * Generate a single read transaction with the given read concern distribution
     * and the timestamp of the last write (for causal ordering).
     */
    private static ClientMessage generateReadTransaction(TimeStampProto lastWriteTs, ReadClass readClass) {
        String txId = UUID.randomUUID().toString();
        String accName = "user" + random.nextInt(100);
        long now = System.currentTimeMillis();

        ReadConcern readConcern;
        ReadLevel readLevel;
        switch (readClass) {
            case LINEARIZABLE:
                readConcern = ReadConcern.forNumber(0);
                readLevel = ReadLevel.MAJORITY;
                break;
            case CAUSAL_LOCAL:
                readConcern = ReadConcern.forNumber(1);
                readLevel = ReadLevel.LOCAL;
                break;
            case CAUSAL_MAJORITY:
                readConcern = ReadConcern.forNumber(1);
                readLevel = ReadLevel.MAJORITY;
                break;
            case EVENTUAL:
            default:
                readConcern = ReadConcern.forNumber(2);
                readLevel = ReadLevel.LOCAL;
                break;
        }

        int applicationId = 1 + random.nextInt(3);

        // LINEARIZABLE reads must go through Raft with majority writeConcern
        int majority = (NUM_OF_SERVERS / 2) + 1;
        int writeConcern = (readClass == ReadClass.LINEARIZABLE) ? majority : 0;

        Transaction transaction = Transaction.newBuilder()
                .setId(txId)
                .setIsReadOnly(true)
                .setAccNameToRead(accName)
                .setReadConcern(readConcern)
                .setReadLevel(readLevel)
                .setMinRequiredConsistency(writeConcern)
                .setWriteConcern(writeConcern)
                .setApplicationId(applicationId)
                .setBaseProfit((double) applicationId)
                .setTransactionSendTimeInMs(now)
                .build();

        return ClientMessage.newBuilder()
                .setT(transaction)
                .setWriteConcern(writeConcern)
                .setTimeStamp(lastWriteTs)
                .setCallbackHost("localhost")
                .setCallbackPort(clientCallbackPort)
                .build();
    }

    private static int computeOperationCount(int total,
                                             Map<?, Double> primaryDistribution,
                                             Map<?, Double> secondaryDistribution) {
        double primary = sumPositiveWeights(primaryDistribution);
        double secondary = sumPositiveWeights(secondaryDistribution);
        double combined = primary + secondary;
        if (combined <= 0.0 || total <= 0) {
            return 0;
        }
        return (int) Math.round(total * (primary / combined));
    }

    private static double sumPositiveWeights(Map<?, Double> distribution) {
        if (distribution == null || distribution.isEmpty()) {
            return 0.0;
        }
        return distribution.values().stream()
                .mapToDouble(v -> Math.max(0.0, v))
                .sum();
    }

    private static <T> Map<T, Double> normalizeDistribution(Map<T, Double> distribution) {
        Map<T, Double> normalized = new HashMap<>();
        if (distribution == null || distribution.isEmpty()) {
            return normalized;
        }

        double sum = distribution.values().stream()
                .mapToDouble(v -> Math.max(0.0, v))
                .sum();

        if (sum <= 0.0) {
            return normalized;
        }

        for (Map.Entry<T, Double> entry : distribution.entrySet()) {
            normalized.put(entry.getKey(), Math.max(0.0, entry.getValue()) / sum);
        }
        return normalized;
    }

    private static List<ReadClass> buildDeterministicReadChoices(int total,
                                                                 Map<ReadClass, Double> distribution,
                                                                 ReadClass fallbackKey) {
        List<ReadClass> result = new ArrayList<>(Math.max(0, total));
        if (total <= 0 || distribution == null || distribution.isEmpty()) {
            return result;
        }

        Map<ReadClass, Integer> counts = new HashMap<>();
        Map<ReadClass, Double> fractions = new HashMap<>();
        int assigned = 0;

        for (Map.Entry<ReadClass, Double> entry : distribution.entrySet()) {
            ReadClass key = entry.getKey();
            double raw = Math.max(0.0, entry.getValue()) * total;
            int base = (int) Math.floor(raw);
            counts.put(key, base);
            fractions.put(key, raw - base);
            assigned += base;
        }

        int remaining = total - assigned;
        List<ReadClass> keysByFraction = new ArrayList<>(distribution.keySet());
        keysByFraction.sort((a, b) -> {
            int cmp = Double.compare(fractions.getOrDefault(b, 0.0), fractions.getOrDefault(a, 0.0));
            return (cmp != 0) ? cmp : a.name().compareTo(b.name());
        });

        int idx = 0;
        while (remaining > 0 && !keysByFraction.isEmpty()) {
            ReadClass key = keysByFraction.get(idx % keysByFraction.size());
            counts.put(key, counts.getOrDefault(key, 0) + 1);
            remaining--;
            idx++;
        }

        List<ReadClass> sortedKeys = new ArrayList<>(counts.keySet());
        sortedKeys.sort((a, b) -> a.name().compareTo(b.name()));
        for (ReadClass key : sortedKeys) {
            int c = counts.getOrDefault(key, 0);
            for (int i = 0; i < c; i++) {
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

    private static int findLeaderId(ServerImpl leader) {
        if (leader == null) {
            return -1;
        }
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            if (injector.getServerById(i) == leader) {
                return i;
            }
        }
        return -1;
    }

    private static int selectReadTargetServerId(ReadClass readClass, int leaderId) {
        if (readClass == ReadClass.LINEARIZABLE) {
            return leaderId;
        }

        if (readClass == ReadClass.CAUSAL_LOCAL) {
            if (NUM_OF_SERVERS <= 1) {
                return leaderId;
            }
            int followerIndex = Math.floorMod(followerReadCursor.getAndIncrement(), NUM_OF_SERVERS - 1);
            for (int i = 0, seen = 0; i < NUM_OF_SERVERS; i++) {
                if (i == leaderId) {
                    continue;
                }
                if (seen == followerIndex) {
                    return i;
                }
                seen++;
            }
            return leaderId;
        }

        return Math.floorMod(anyServerReadCursor.getAndIncrement(), NUM_OF_SERVERS);
    }

    /**
     * Build deterministic level choices for a fixed count using a weighted distribution.
     */
    private static List<Integer> buildDeterministicChoices(int total,
                                                           Map<Integer, Double> distribution,
                                                           int fallbackKey) {
        List<Integer> result = new ArrayList<>(Math.max(0, total));
        if (total <= 0 || distribution == null || distribution.isEmpty()) {
            return result;
        }

        Map<Integer, Integer> counts = new HashMap<>();
        Map<Integer, Double> fractions = new HashMap<>();
        int assigned = 0;

        for (Map.Entry<Integer, Double> entry : distribution.entrySet()) {
            int key = entry.getKey();
            double raw = Math.max(0.0, entry.getValue()) * total;
            int base = (int) Math.floor(raw);
            counts.put(key, base);
            fractions.put(key, raw - base);
            assigned += base;
        }

        int remaining = total - assigned;
        List<Integer> keysByFraction = new ArrayList<>(distribution.keySet());
        keysByFraction.sort((a, b) -> {
            int cmp = Double.compare(fractions.getOrDefault(b, 0.0), fractions.getOrDefault(a, 0.0));
            return (cmp != 0) ? cmp : Integer.compare(a, b);
        });

        int idx = 0;
        while (remaining > 0 && !keysByFraction.isEmpty()) {
            int key = keysByFraction.get(idx % keysByFraction.size());
            counts.put(key, counts.getOrDefault(key, 0) + 1);
            remaining--;
            idx++;
        }

        List<Integer> sortedKeys = new ArrayList<>(counts.keySet());
        sortedKeys.sort(Integer::compareTo);
        for (int key : sortedKeys) {
            int c = counts.getOrDefault(key, 0);
            for (int i = 0; i < c; i++) {
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

    private static int findAvailablePort(int startPort, int endPortInclusive) throws IOException {
        for (int port = startPort; port <= endPortInclusive; port++) {
            try (ServerSocket ignored = new ServerSocket(port)) {
                return port;
            } catch (IOException ignored) {
                // Try the next port.
            }
        }
        throw new IOException("No free callback port found in range " + startPort + "-" + endPortInclusive);
    }

    private static TimeStampProto getLatestAckedTimestamp() {
        if (clientServerImpl != null) {
            return clientServerImpl.getLastTimeStampProto();
        }
        return lastWriteTimestamp;
    }

    /**
     * Select read concern based on probability distribution
     */
    private static int selectReadConcern(Map<Integer, Double> distribution) {
        double rand = random.nextDouble();
        double cumulative = 0.0;
        for (Map.Entry<Integer, Double> entry : distribution.entrySet()) {
            cumulative += entry.getValue();
            if (rand <= cumulative) {
                return entry.getKey();
            }
        }
        return 2; // default EVENTUAL
    }

    /**
     * Select write concern based on probability distribution
     */
    private static int selectWriteConcern(Map<Integer, Double> distribution) {
        double rand = random.nextDouble();
        double cumulative = 0.0;

        for (Map.Entry<Integer, Double> entry : distribution.entrySet()) {
            cumulative += entry.getValue();
            if (rand <= cumulative) {
                return entry.getKey();
            }
        }

        // Default to W:1 if distribution doesn't sum to 1
        return 1;
    }
    
    /**
     * Clear all CSV files at startup to ensure clean data collection
     */
    private static void clearCSVFiles() {
        // Per-server CSV files (one file per server ID)
        String[] perServerFiles = {
            "tps_%d.csv",
            "writeconcern_frequency_%d.csv",
            "writeconcern_tps_%d.csv",
            "batch_mix_before_after_%d.csv",
            "read_requests_%d.csv",
            "backlog_%d.csv",
            "avg_latencies_%d.csv",
            "incoming_transaction_rate_%d.csv",
            "system_latency_%d.csv",
            "backlog_samples_%d.csv",
            "read_latencies_%d.csv"
        };

        // Global (non-server-specific) CSV files
        String[] globalFiles = {
            "writeconcern.csv",
            "token_costs.csv",
            "lab1_Test.csv",
            "final_batch_avg_tps_log.csv",
            "incoming_transaction_rate_global.csv"
        };

        for (int sid = 0; sid < NUM_OF_SERVERS; sid++) {
            for (String pattern : perServerFiles) {
                String filename = String.format(pattern, sid);
                File file = new File(filename);
                if (file.exists()) {
                    if (file.delete()) {
                        System.out.println("Cleared CSV file: " + filename);
                    } else {
                        System.out.println("Warning: Could not delete " + filename);
                    }
                }
            }
        }

        for (String filename : globalFiles) {
            File file = new File(filename);
            if (file.exists()) {
                if (file.delete()) {
                    System.out.println("Cleared CSV file: " + filename);
                } else {
                    System.out.println("Warning: Could not delete " + filename);
                }
            }
        }
    }
}








