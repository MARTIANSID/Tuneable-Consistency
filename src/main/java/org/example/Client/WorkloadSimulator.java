package org.example.Client;


import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.ds.paxos.ClientMessage;
import org.ds.paxos.RaftGrpc;
import org.ds.paxos.Transaction;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * WorkloadSimulator drives a sequence of traffic phases to emulate a realistic production style pattern:
 * 1. Warm-up: Mostly light (writeConcern 1) transactions at a high sustained rate so the leader quickly meets
 *    the min throughput threshold and has abundant tokens to upgrade many transactions opportunistically.
 * 2. Heavy Spike: Sudden surge dominated by higher write concerns (3 & majority(5)) — purposefully exhausting
 *    token bucket and forcing throttling/backlog growth in the server.
 * 3. Stabilization: Mixed distribution with moderate variability where the system should converge and keep
 *    backlog under control as token refill & adaptive costing settle.
 *
 * You can extend / tweak phases or supply your own via code changes / future CLI args.
 */
public class WorkloadSimulator {

    /** Configuration for a single phase */
    public static class Phase {
        final String name;
        final int durationSeconds;
        final int targetTps;              // Mean target TPS for the phase
        final double tpsJitterFraction;   // 0.2 -> +/-20% jitter applied at each 1s boundary
        final Map<Integer, Double> writeConcernDistribution; // minRequiredConsistency distribution
        final double extraIntermediateProfit; // profit added per level between minRequiredConsistency and majority (exclusive)
        final double extraMajorityProfit;     // profit added if transaction is upgraded to majority

        public Phase(String name,
                     int durationSeconds,
                     int targetTps,
                     double tpsJitterFraction,
                     Map<Integer, Double> writeConcernDistribution,
                     double extraIntermediateProfit,
                     double extraMajorityProfit) {
            this.name = name;
            this.durationSeconds = durationSeconds;
            this.targetTps = targetTps;
            this.tpsJitterFraction = tpsJitterFraction;
            this.writeConcernDistribution = writeConcernDistribution;
            this.extraIntermediateProfit = extraIntermediateProfit;
            this.extraMajorityProfit = extraMajorityProfit;
        }
    }

    private final List<Phase> phases = new ArrayList<>();
    private final List<Integer> serverPorts; // candidate server ports (we try in round-robin; leader forwarding handles non-leaders)
    private final int majorityLevel;
    private final Random random = new Random();
    private final AtomicLong idGen = new AtomicLong(System.nanoTime());
    private final ExecutorService sendPool;
    private final int concurrency;

    // Each second we break into smaller ticks for smoother pacing (e.g. 100ms -> 10 ticks / second)
    private static final int TICK_MS = 100;

    public WorkloadSimulator(List<Integer> serverPorts, int numServers, int concurrency) {
        this.serverPorts = serverPorts;
        this.majorityLevel = (numServers / 2) + 1;
        this.concurrency = concurrency;
        this.sendPool = Executors.newFixedThreadPool(concurrency);
    }

    public WorkloadSimulator addPhase(Phase p) {
        phases.add(p);
        return this;
    }

    private int pickMinConsistency(Map<Integer, Double> dist) {
        double total = dist.values().stream().mapToDouble(Double::doubleValue).sum();
        double r = random.nextDouble() * total;
        double cum = 0.0;
        for (Map.Entry<Integer, Double> e : dist.entrySet()) {
            cum += e.getValue();
            if (r <= cum) return e.getKey();
        }
        // fallback (shouldn't happen unless rounding)
        return dist.keySet().iterator().next();
    }

    private ManagedChannel newChannel(int port) {
        return ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
    }

    private void sendTransaction(RaftGrpc.RaftBlockingStub stub, int minConsistency, double extraIntermediateProfit, double extraMajorityProfit) {
        long nowMs = System.currentTimeMillis();
        long id = idGen.incrementAndGet();

        Transaction tx = Transaction.newBuilder()
                .setId(Long.toString(id))
                .setAmount(1)
                .setReceiver("AcctB")
                .setSender("AcctA")
                .setMinRequiredConsistency(minConsistency)
                .setBaseProfit(minConsistency) // simple base profit heuristic
                .setExtraIntermediateProfit(minConsistency == majorityLevel ? 0 : extraIntermediateProfit)
                .setExtraProfitMajority(minConsistency == majorityLevel ? 0 : extraMajorityProfit)
                .setTransactionSendTimeInMs(nowMs)
                .build();

        ClientMessage msg = ClientMessage.newBuilder()
                .setT(tx)
                // initial writeConcern = minConsistency; server may upgrade if profitable
                .setWriteConcern(minConsistency)
                .build();
        try {
            stub.sendTransaction(msg); // fire & forget (blocking call but quick)
        } catch (Exception ignored) {
            // We ignore for now; real implementation could retry with another server port.
        }
    }

    public void run() {
        if (phases.isEmpty()) {
            System.err.println("No phases configured.");
            return;
        }
        System.out.printf("Starting workload with %d phases. Majority=%d%n", phases.size(), majorityLevel);

        int portIndex = 0; // round-robin over serverPorts
        for (Phase phase : phases) {
            System.out.printf("\n=== Phase: %s | duration=%ds | targetTPS=%d ===%n", phase.name, phase.durationSeconds, phase.targetTps);
            long phaseEnd = System.currentTimeMillis() + phase.durationSeconds * 1000L;

            int ticksPerSecond = 1000 / TICK_MS;
            while (System.currentTimeMillis() < phaseEnd) {
                // Derive current second target with jitter
                double jitter = 1 + (random.nextDouble() * 2 - 1) * phase.tpsJitterFraction; // (1 - f) .. (1 + f)
                int secondTarget = (int) Math.max(1, Math.round(phase.targetTps * jitter));
                int perTickBase = secondTarget / ticksPerSecond;
                int remainder = secondTarget % ticksPerSecond;

                long secondBoundary = System.currentTimeMillis() + 1000;
                for (int tick = 0; tick < ticksPerSecond; tick++) {
                    if (System.currentTimeMillis() >= phaseEnd) break; // phase done early
                    // Distribute remainder across initial ticks
                    int toSend = perTickBase + (tick < remainder ? 1 : 0);
                    if (toSend > 0) {
                        // Acquire / rotate channel
                        int port = serverPorts.get(portIndex);
                        portIndex = (portIndex + 1) % serverPorts.size();
                        ManagedChannel channel = newChannel(port);
                        RaftGrpc.RaftBlockingStub stub = RaftGrpc.newBlockingStub(channel);
                        CountDownLatch latch = new CountDownLatch(toSend);
                        for (int i = 0; i < toSend; i++) {
                            sendPool.submit(() -> {
                                try {
                                    int minCons = pickMinConsistency(phase.writeConcernDistribution);
//                                    int minCons = 5;
                                    sendTransaction(stub, minCons, phase.extraIntermediateProfit, phase.extraMajorityProfit);
                                } finally {
                                    latch.countDown();
                                }
                            });
                        }
                        try { latch.await(800, TimeUnit.MILLISECONDS); } catch (InterruptedException ignored) {}
                        channel.shutdown();
                    }
                    try { Thread.sleep(TICK_MS ); } catch (InterruptedException ignored) { }
                }
                // Sleep the remainder of the second if we were fast (coarse pacing)
                long leftover = secondBoundary - System.currentTimeMillis();
                if (leftover > 5) {
                    try { Thread.sleep(Math.min(leftover, 50)); } catch (InterruptedException ignored) {}
                }
            }
            System.out.printf("Completed phase %s at %s%n", phase.name, Instant.now());
        }

        sendPool.shutdown();
        try { sendPool.awaitTermination(5, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
        System.out.println("All phases complete. Workload simulator exiting.");
    }

    public static void main(String[] args) {
        // Basic args (optional): numServers, concurrency. Defaults if absent.
        int numServers = args.length > 0 ? Integer.parseInt(args[0]) : 9;
        int concurrency = args.length > 1 ? Integer.parseInt(args[1]) : 16;

        // Candidate server ports (adjust if your cluster uses different ones). Servers typically start at 8001 .. 800N
        List<Integer> ports = new ArrayList<>();
        for (int i = 1; i <= numServers; i++) {
            ports.add(8000 + i);
        }

        WorkloadSimulator simulator = new WorkloadSimulator(ports, numServers, concurrency);

        // Phase 1: Warm-up (high rate, mostly WC=1, encourage upgrades via profit incentives)
        simulator.addPhase(new Phase(
                "WarmUp", 15, 2000, 0.15,
                mapOf(
                        1, 0.75,
                        2, 0.10,
                        3, 0.10,
                        (numServers / 2) + 1, 0.05
                ),
                0.5, // extraIntermediateProfit
                0.5  // extraMajorityProfit
        ));

        // Phase 2: Heavy Spike (dominantly higher write concerns 3 & majority to drain tokens and cause backlog)
        simulator.addPhase(new Phase(
                "Spike", 8, 520, 0.25,
                mapOf(
                        1, 0.10,
                        3, 0.50,
                        (numServers / 2) + 1, 0.40
                ),
                0.3,
                0.4
        ));

        // Phase 3: Stabilization (mixed workload, moderate rate)
        simulator.addPhase(new Phase(
                "Stabilize", 25, 300, 0.20,
                mapOf(
                        1, 0.50,
                        2, 0.20,
                        3, 0.20,
                        (numServers / 2) + 1, 0.10
                ),
                0.4,
                0.4
        ));

        simulator.run();
    }

    // Convenience for inline map creation using varargs (k1,v1,k2,v2,...)
    public static Map<Integer, Double> mapOf(Object... kv) {
        if (kv.length % 2 != 0) throw new IllegalArgumentException("key/value length mismatch");
        Map<Integer, Double> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((Integer) kv[i], ((Number) kv[i + 1]).doubleValue());
        }
        return m;
    }
}
