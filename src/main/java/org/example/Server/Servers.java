package org.example.Server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
        // Phase 1: WarmUp - 100% W:1
        PHASES.add(new Phase("WarmUp", 50, 20000, Map.of(1, 1.0, 2, 0.0)));
        
        // Phase 2: Spike - 10% W:1, 90% W:2
        PHASES.add(new Phase("Spike", 20, 20000, Map.of(1, 0.40, 2, 0.60)));
        
        // Phase 3: Stabilize - 50% W:1, 50% W:2
        PHASES.add(new Phase("Stabilize", 100, 20000, Map.of(1, 0.50, 2, 0.50)));
    }

    private static final int BATCH_SIZE = 1000;  // Transactions per batch (increased to reduce gRPC overhead)
    private static final Random random = new Random();

    public static void main(String[] args) throws IOException{
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
     * Starts phased transaction injection similar to StressTest.java
     * Runs phases in a continuous cycle
     */
    private static void startPhasedInjection() {
        new Thread(() -> {
            System.out.println("⏳ Waiting for leader election...");
            ServerImpl leader = injector.waitForLeader(10000);
            if (leader == null) {
                System.err.println("❌ No leader elected, aborting injection");
                return;
            }

            System.out.println("🚀 Starting phased transaction injection...");
            printPhaseConfig();

            // Run phases in continuous cycle
            while (true) {
                for (Phase phase : PHASES) {
                    runPhase(phase);
                }
                System.out.println("🔄 Cycle complete, restarting phases...\n");
            }
        }).start();
    }

    /**
     * Run a single phase
     */
    private static void runPhase(Phase phase) {
        System.out.println("\n========================================");
        System.out.printf("📌 Starting Phase: %s%n", phase.name);
        System.out.printf("   Duration: %ds | Target TPS: %d%n", phase.durationSeconds, phase.targetTPS);
        System.out.printf("   WC Distribution: %s%n", phase.writeConcernDistribution);
        System.out.println("========================================");

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        
        // Calculate injection interval
        int batchesPerSecond = Math.max(1, phase.targetTPS / BATCH_SIZE);
        long intervalMs = 1000 / batchesPerSecond;
        int actualBatchSize = phase.targetTPS / batchesPerSecond;

        long phaseStartTime = System.currentTimeMillis();
        long phaseEndTime = phaseStartTime + (phase.durationSeconds * 1000L);
        
        AtomicInteger totalInjected = new AtomicInteger(0);

        scheduler.scheduleAtFixedRate(() -> {
            if (System.currentTimeMillis() >= phaseEndTime) {
                scheduler.shutdown();
                return;
            }

            // Generate batch with phase's write concern distribution
            List<ClientMessage> batch = generateTransactionBatch(actualBatchSize, phase.writeConcernDistribution);
            injector.injectIntoLeader(batch);
            totalInjected.addAndGet(batch.size());

        }, 0, intervalMs, TimeUnit.MILLISECONDS);

        // Wait for phase to complete
        try {
            scheduler.awaitTermination(phase.durationSeconds + 5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.printf("✅ Phase '%s' complete | Total injected: %d%n", phase.name, totalInjected.get());
        injector.printAllServersStatus();
    }

    /**
     * Print phase configuration
     */
    private static void printPhaseConfig() {
        System.out.println("\n📊 Workload Phases Configuration:");
        for (int i = 0; i < PHASES.size(); i++) {
            Phase p = PHASES.get(i);
            System.out.printf("   Phase %d: %s | %ds | %d TPS | WC: %s%n",
                    i + 1, p.name, p.durationSeconds, p.targetTPS, p.writeConcernDistribution);
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
}








