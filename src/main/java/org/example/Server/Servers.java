package org.example.Server;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
    
    // Static reference to TransactionInjector for testing
    public static TransactionInjector injector;

    // ========== Workload Phases (similar to StressTest.java) ==========
    static class Phase {
        String name;
        int durationSeconds;
        int targetTPS;
        Map<Integer, Double> writeConcernDistribution; // wc -> probability

        Phase(String name, int durationSeconds, int targetTPS, Map<Integer, Double> wcDist) {
            this.name = name;
            this.durationSeconds = durationSeconds;
            this.targetTPS = targetTPS;
            this.writeConcernDistribution = wcDist;
        }
    }

    private static final List<Phase> PHASES = new ArrayList<>();
    static {
        // Light workload - mostly W:1 (2 seconds)
        PHASES.add(new Phase("Light", 10, 7000, Map.of(1, 0.60, 2, 0.20)));
        
        // Heavy workload - mostly W:2 (2 seconds)
        PHASES.add(new Phase("Heavy", 10, 7000, Map.of(1, 0.30, 2, 0.70)));
        
        // Mixed workload - balanced W:1/W:2 (2 seconds)
        PHASES.add(new Phase("Mixed", 10, 7000, Map.of(1, 0.50, 2, 0.50)));
    }
    
    // Track current phase index for sequential execution
    private static final AtomicInteger currentPhaseIndex = new AtomicInteger(0);

    private static final int BATCH_SIZE = 1000;  // Transactions per batch (increased to reduce gRPC overhead)
    private static final Random random = new Random();

    // Read transaction configuration
    private static final double READ_RATIO = 0.50; // 50% of batch are reads
    // Leader reads: mix of EVENTUAL + LINEARIZABLE
    private static final Map<Integer, Double> LEADER_READ_CONCERN_DIST = Map.of(
            0, 0.50,  // LINEARIZABLE
            2, 0.50   // EVENTUAL
    );
    // Replica reads: mix of EVENTUAL + CAUSAL (50/50)
    private static final Map<Integer, Double> REPLICA_READ_CONCERN_DIST = Map.of(
            1, 0.50,  // CAUSAL
            2, 0.50   // EVENTUAL
    );
    // Track last write timestamp for causal/linearizable reads
    private static volatile TimeStampProto lastWriteTimestamp = TimeStampProto.newBuilder()
            .setP(System.currentTimeMillis()).setL(0).build();
    
    // Experiment parameters
    private static final long TOTAL_EXPERIMENT_DURATION_MS = 40000;  // 150 seconds total
    private static volatile long experimentStartTime;
    private static volatile boolean experimentRunning = true;
    
    // Current phase parameters (volatile for thread-safe reads)
    private static volatile Phase currentPhase = PHASES.get(1);  // Start with Mixed
    private static volatile long phaseEndTime = 0;
    private static final Object phaseLock = new Object();
    
    // Incoming transaction tracking
    private static final Queue<Long> incomingTransactionTimestamps = new LinkedList<>();
    private static final Object incomingTransactionLock = new Object();
    private static final AtomicLong lastPrintTime = new AtomicLong(0);

    public static void main(String[] args) throws IOException{
        // Clear all CSV files at startup
        clearCSVFiles();
        
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
     * Starts phased transaction injection with TWO separate threads:
     * 1. Phase Manager - changes phase parameters periodically
     * 2. Injector - continuously injects based on current phase parameters
     * 
     * Total experiment duration: 30 seconds
     */
    private static void startPhasedInjection() {
        System.out.println("⏳ Waiting for leader election...");
        ServerImpl leader = injector.waitForLeader(10000);
        if (leader == null) {
            System.err.println("❌ No leader elected, aborting injection");
            return;
        }

        System.out.println("🚀 Starting 20-second randomized workload experiment...");
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
                        System.out.println("\n🏁 20-second experiment completed!");
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
        
        // Thread 2: Continuous Injector - runs until experiment ends
        Thread continuousInjector = new Thread(() -> {
            AtomicInteger totalInjected = new AtomicInteger(0);
            long lastReportTime = System.currentTimeMillis();
            
            while (!Thread.currentThread().isInterrupted() && experimentRunning) {
                try {
                    // Check if experiment is still running
                    if (!experimentRunning) {
                        break;
                    }
                    
                    // Read current phase parameters (volatile read - thread safe)
                    Phase phase = currentPhase;
                    
                    // Calculate timing for current phase
                    int batchesPerSecond = Math.max(1, phase.targetTPS / BATCH_SIZE);
                    long intervalMs = 1000 / batchesPerSecond;
                    int actualBatchSize = phase.targetTPS / batchesPerSecond;
                    
                    long batchStart = System.currentTimeMillis();
                    
                    // Generate batch with current phase's distribution
                    List<ClientMessage> batch = generateTransactionBatch(actualBatchSize, phase.writeConcernDistribution);
                    
                    // Inject into any available server (they will forward to leader)
                    boolean injected = injectBatchRobust(batch);
                    
                    if (injected) {
                        totalInjected.addAndGet(batch.size());
                    }
                    
                    // Report every 3 seconds (reduced for 30-second experiment)
                    long currentTime = System.currentTimeMillis();
                    long elapsedSeconds = (currentTime - experimentStartTime) / 1000;
                    if (currentTime - lastReportTime >= 3000) {
                        System.out.printf("📊 [%02ds] Total=%d | Phase=%s | TPS=%d%n",
                                elapsedSeconds, totalInjected.get(), phase.name, phase.targetTPS);
                        lastReportTime = currentTime;
                    }
                    
                    // Sleep to maintain target rate
                    long elapsed = System.currentTimeMillis() - batchStart;
                    long sleepTime = intervalMs - elapsed;
                    if (sleepTime > 0) {
                        Thread.sleep(sleepTime);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            
            System.out.printf("\n🏆 Experiment completed! Total injected: %d transactions%n", 
                    totalInjected.get());
            
            // Exit the program after experiment completes
            System.out.println("\n✅ Shutting down servers...");
            System.exit(0);
        }, "ContinuousInjector");
        continuousInjector.setDaemon(true);
        continuousInjector.start();

        // Thread 3: Replica Read Injector - sends EVENTUAL + CAUSAL reads to followers
        Thread replicaReadInjector = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted() && experimentRunning) {
                try {
                    if (!experimentRunning) break;

                    Phase phase = currentPhase;
                    int readTPS = (int) (phase.targetTPS * READ_RATIO);
                    int batchesPerSecond = Math.max(1, readTPS / BATCH_SIZE);
                    long intervalMs = 1000 / batchesPerSecond;
                    int actualBatchSize = readTPS / batchesPerSecond;

                    long batchStart = System.currentTimeMillis();
                    TimeStampProto lastWriteTs = lastWriteTimestamp;

                    // Pick a random follower (not the leader)
                    ServerImpl currentLeader = injector.getLeader();
                    List<ServerImpl> followers = new ArrayList<>();
                    for (int i = 0; i < NUM_OF_SERVERS; i++) {
                        ServerImpl s = injector.getServerById(i);
                        if (s != null && s != currentLeader) {
                            followers.add(s);
                        }
                    }

                    if (!followers.isEmpty()) {
                        ServerImpl follower = followers.get(random.nextInt(followers.size()));
                        List<TransactionOption> readBatch = new ArrayList<>();
                        TimeStampProto lastWriteTs2 = lastWriteTs;
                        for (int i = 0; i < actualBatchSize; i++) {
                            ClientMessage readMsg = generateReadTransaction(lastWriteTs2, REPLICA_READ_CONCERN_DIST);
                            readBatch.add(TransactionOption.fromClientMessage(readMsg));
                        }
                        follower.batchLock.lock();
                        try {
                            follower.batchOfTransactions.addAll(readBatch);
                        } finally {
                            follower.batchLock.unlock();
                        }
                        trackIncomingTransactions(actualBatchSize);
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
                    // Log but don't crash — follower may not be ready yet
                    System.err.println("Replica read injection error: " + e.getMessage());
                    try { Thread.sleep(500); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }, "ReplicaReadInjector");
        replicaReadInjector.setDaemon(true);
        replicaReadInjector.start();
    }
    
    /**
     * Select a new phase and update the shared phase parameters atomically
     * Phases run SEQUENTIALLY to guarantee all are experienced
     */
    private static void selectAndStartNewPhase() {
        if (!experimentRunning) return;
        
        Phase newPhase = selectNextPhase();
        long newEndTime = System.currentTimeMillis() + (newPhase.durationSeconds * 1000L);
        
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
        System.out.printf("   Duration: %ds | Target TPS: %d%n", newPhase.durationSeconds, newPhase.targetTPS);
        System.out.printf("   WC Distribution: %s%n", newPhase.writeConcernDistribution);
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
        int index = currentPhaseIndex.getAndIncrement();
        if (index >= PHASES.size()) {
            // Cycle back to first phase if needed
            currentPhaseIndex.set(1);
            index = 0;
        }
        return PHASES.get(index);
    }

    /**
     * Print phase configuration for 20-second experiment
     */
    private static void printPhaseConfig() {
        System.out.println("\n📊 20-Second Experiment Configuration:");
        System.out.println("   Total Duration: 20 seconds");
        System.out.println("   Phases run sequentially:");
        int totalDuration = 0;
        for (Phase p : PHASES) {
            System.out.printf("   %d. %s: %ds | %d TPS | WC: %s%n",
                    PHASES.indexOf(p) + 1, p.name, p.durationSeconds, p.targetTPS, p.writeConcernDistribution);
            totalDuration += p.durationSeconds;
        }
        System.out.printf("   Total phase duration: %ds (buffer: %ds)%n", totalDuration, 
                (int)(TOTAL_EXPERIMENT_DURATION_MS/1000) - totalDuration);
        System.out.println();
    }

    /**
     * Generate a batch of transactions with specified write concern distribution
     */
    private static List<ClientMessage> generateTransactionBatch(int size, Map<Integer, Double> wcDistribution) {
        List<ClientMessage> batch = new ArrayList<>(size);
        long now = System.currentTimeMillis();

        int readCount = (int) (size * READ_RATIO);
        int writeCount = size - readCount;

        // Generate write transactions first
        for (int i = 0; i < writeCount; i++) {
            int minConsistency = selectWriteConcern(wcDistribution);

            String txId = UUID.randomUUID().toString();
            String sender = "user" + (i % 100);
            String receiver = "user" + ((i + 50) % 100);
            double amount = 1.0 + (i % 10);

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
                    .setL(i)
                    .build();

            ClientMessage message = ClientMessage.newBuilder()
                    .setT(transaction)
                    .setWriteConcern(minConsistency)
                    .setTimeStamp(ts)
                    .setCallbackHost("localhost")
                    .setCallbackPort(9000)
                    .build();

            batch.add(message);
            // Track last write timestamp for read transactions
            lastWriteTimestamp = ts;
        }

        // Generate read transactions for leader (EVENTUAL + LINEARIZABLE)
        TimeStampProto lastWriteTs = lastWriteTimestamp;
        for (int i = 0; i < readCount; i++) {
            batch.add(generateReadTransaction(lastWriteTs, LEADER_READ_CONCERN_DIST));
        }

        return batch;
    }

    /**
     * Generate a single read transaction with the given read concern distribution
     * and the timestamp of the last write (for causal ordering).
     */
    private static ClientMessage generateReadTransaction(TimeStampProto lastWriteTs,
                                                          Map<Integer, Double> rcDistribution) {
        String txId = UUID.randomUUID().toString();
        String accName = "user" + random.nextInt(100);
        long now = System.currentTimeMillis();

        int readConcernOrdinal = selectReadConcern(rcDistribution);
        ReadConcern readConcern = ReadConcern.forNumber(readConcernOrdinal);
        // LINEARIZABLE -> MAJORITY level, others -> LOCAL
        ReadLevel readLevel = (readConcernOrdinal == 0) ? ReadLevel.MAJORITY : ReadLevel.LOCAL;

        int applicationId = 1 + random.nextInt(3);

        Transaction transaction = Transaction.newBuilder()
                .setId(txId)
                .setIsReadOnly(true)
                .setAccNameToRead(accName)
                .setReadConcern(readConcern)
                .setReadLevel(readLevel)
                .setApplicationId(applicationId)
                .setBaseProfit((double) applicationId)
                .setTransactionSendTimeInMs(now)
                .build();

        return ClientMessage.newBuilder()
                .setT(transaction)
                .setWriteConcern(0)
                .setTimeStamp(lastWriteTs)
                .setCallbackHost("localhost")
                .setCallbackPort(9000)
                .build();
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








