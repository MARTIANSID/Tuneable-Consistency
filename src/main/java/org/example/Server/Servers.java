package org.example.Server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.ds.paxos.ClientMessage;
import org.ds.paxos.Transaction;
import org.ds.paxos.TimeStampProto;

public class Servers{

    // set the number of servers from here
    public static final int NUM_OF_SERVERS =  3;
    
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
        // Light workload (20% probability) - Low TPS, mostly W:1
        PHASES.add(new Phase("Light", 8, 50000, Map.of(1, 0.80, 2, 0.20)));
        
        // Mixed workload (60% probability) - Medium TPS, balanced W:1/W:2
        PHASES.add(new Phase("Mixed", 10, 50000, Map.of(1, 0.50, 2, 0.50)));
        
        // Heavy workload (20% probability) - High TPS, mostly W:2
        PHASES.add(new Phase("Heavy", 8, 50000, Map.of(1, 0.30, 2, 0.70)));
    }
    
    // Probability weights for phase selection (must sum to 1.0)
    private static final double[] PHASE_PROBABILITIES = {0.20, 0.60, 0.20};

    private static final int BATCH_SIZE = 1000;  // Transactions per batch (increased to reduce gRPC overhead)
    private static final Random random = new Random();
    
    // Experiment parameters
    private static final long TOTAL_EXPERIMENT_DURATION_MS = 30000;  // 30 seconds total
    private static volatile long experimentStartTime;
    private static volatile boolean experimentRunning = true;
    
    // Current phase parameters (volatile for thread-safe reads)
    private static volatile Phase currentPhase = PHASES.get(1);  // Start with Mixed
    private static volatile long phaseEndTime = 0;
    private static final Object phaseLock = new Object();

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

        System.out.println("🚀 Starting 30-second randomized workload experiment...");
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
                        System.out.println("\n🏁 30-second experiment completed!");
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
        }, "ContinuousInjector");
        continuousInjector.setDaemon(true);
        continuousInjector.start();
    }
    
    /**
     * Select a new phase and update the shared phase parameters atomically
     * Only creates new phases if experiment is still running
     */
    private static void selectAndStartNewPhase() {
        if (!experimentRunning) return;
        
        Phase newPhase = selectRandomPhase();
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
        System.out.println("\n========================================");
        System.out.printf("📌 Starting Phase: %s (Experiment time remaining: %ds)%n", newPhase.name, remainingSeconds);
        System.out.printf("   Duration: %ds | Target TPS: %d%n", newPhase.durationSeconds, newPhase.targetTPS);
        System.out.printf("   WC Distribution: %s%n", newPhase.writeConcernDistribution);
        System.out.println("========================================");
    }
    
    /**
     * Robust batch injection - tries multiple servers if needed
     */
    private static boolean injectBatchRobust(List<ClientMessage> batch) {
        // First try: current leader
        ServerImpl leader = injector.getLeader();
        if (leader != null) {
            leader.batchLock.lock();
            try {
                leader.batchOfTransactions.addAll(batch);
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
                    server.batchOfTransactions.addAll(batch);
                    return true;
                } finally {
                    server.batchLock.unlock();
                }
            }
        }
        
        return false;
    }

    /**
     * Select a random phase based on probability weights
     */
    private static Phase selectRandomPhase() {
        double rand = random.nextDouble();
        double cumulative = 0.0;
        
        for (int i = 0; i < PHASES.size(); i++) {
            cumulative += PHASE_PROBABILITIES[i];
            if (rand <= cumulative) {
                return PHASES.get(i);
            }
        }
        
        // Fallback to mixed workload (most common)
        return PHASES.get(1);
    }

    /**
     * Print phase configuration for 30-second experiment
     */
    private static void printPhaseConfig() {
        System.out.println("\n📊 30-Second Experiment Configuration:");
        System.out.println("   Total Duration: 30 seconds");
        System.out.println("   Phases selected randomly:");
        for (int i = 0; i < PHASES.size(); i++) {
            Phase p = PHASES.get(i);
            System.out.printf("   %s: %ds | %d TPS | WC: %s | Probability: %.0f%%%n",
                    p.name, p.durationSeconds, p.targetTPS, p.writeConcernDistribution, 
                    PHASE_PROBABILITIES[i] * 100);
        }
        System.out.println();
    }

    /**
     * Generate a batch of transactions with specified write concern distribution
     */
    private static List<ClientMessage> generateTransactionBatch(int size, Map<Integer, Double> wcDistribution) {
        List<ClientMessage> batch = new ArrayList<>(size);
        long now = System.currentTimeMillis();

        for (int i = 0; i < size; i++) {
            // Select write concern based on distribution
            int minConsistency = selectWriteConcern(wcDistribution);

            String txId = UUID.randomUUID().toString();
            String sender = "user" + (i % 100);
            String receiver = "user" + ((i + 50) % 100);
            double amount = 1.0 + (i % 10);

            Transaction transaction = Transaction.newBuilder()
                    .setId(txId)
                    .setSender(sender)
                    .setReceiver(receiver)
                    .setAmount(amount)
                    .setTransactionSendTimeInMs(now)
                    .setMinRequiredConsistency(minConsistency)
                    .setBaseProfit(1.0)
                    .setExtraProfitMajority(0.5)
                    .setExtraIntermediateProfit(0.25)
                    .setWriteConcern(minConsistency)
                    .setIsReadOnly(false)
                    .build();

            ClientMessage message = ClientMessage.newBuilder()
                    .setT(transaction)
                    .setWriteConcern(minConsistency)
                    .setTimeStamp(TimeStampProto.newBuilder()
                            .setP(now)
                            .setL(i)
                            .build())
                    .setCallbackHost("localhost")
                    .setCallbackPort(9000)
                    .build();

            batch.add(message);
        }

        return batch;
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
        String[] csvFiles = {
            "tps.csv",
            "writeconcern_frequency.csv", 
            "writeconcern_tps.csv",
            "writeconcern.csv",
            "backlog.csv",
            "avg_latencies.csv",
            "token_costs.csv",
            "lab1_Test.csv"
        };
        
        for (String filename : csvFiles) {
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








