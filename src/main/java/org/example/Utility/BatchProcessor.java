 
package org.example.Utility;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.Map;
import org.ds.paxos.ReadConcern;
import org.ds.paxos.ReadLevel;
import org.ds.paxos.Transaction;

import org.ds.paxos.ClientMessage;

public class BatchProcessor {

    private final int NUM_OF_SERVERS;
    final double EPS = 1e-9;

    // ========== Warmup Phase Constants ==========
    private static final long WARMUP_DURATION_MS = 7000;  // 7 seconds warmup
    private static final long UPGRADE_WARMUP_MS = 3000;   // disable upgrades for first 3 seconds
    private static volatile long systemStartTime = 0;

    // ========== TPS Constants (Heuristic) ==========
    private static final double MIN_TPS = 30000.0;      // Minimum TPS to maintain
    private static final double UPGRADE_THRESHOLD = 1.15;  // Need 15% headroom to start upgrading
    private static final double UPGRADE_FLOOR = 1.10;      // Stop upgrading at 10% above minTPS
    private static final double MIN_TPS_OF_MAJORITY = 10500.0; // Minimum TPS expected from majority writes


    public BatchProcessor(int numOfServers) {
        this.NUM_OF_SERVERS = numOfServers;
        // Initialize system start time on first BatchProcessor creation
        if (systemStartTime == 0) {
            systemStartTime = System.currentTimeMillis();
            System.out.println("[BatchProcessor] Warmup phase started - will use MIN_HASH_MAP for 7 seconds");
        }
    }


    // we ideally will want to set a min worst case tps for every writeConcern level, this is to avoid very low tps during warm up phases also we ideally expect tps to not fall below these levels
    private static final HashMap<Integer, Double> MIN_HASH_MAP = new HashMap<>() {{
        put(1, 50000.0);
        put(2, 12000.0);
    }};
    
    private static final double writeCost = 8;

    private static final HashMap<Integer, Double> MIN_LATENCY_MAP = new HashMap<>() {{
        put(1, 80.0);  // 50 ms for W:1
        put(2, 3000.0); // 150 ms for W:2
     }};

     private static double MAX_LATENCY = 70.0; // Max average latency in ms
     private static final double UPGRADE_LATENCY_THRESHOLD = 1.0; // Need 15% headroom to start upgrading
     private static final double UPGRADE_LATENCY_FLOOR = 0.95; // Stop upgrading at 10% above max latency
     private static final double MAX_LOAD = 12500;
    

    private static final int RC_KEY_EVENTUAL_ALL = 0;
    private static final int RC_KEY_CAUSAL_LOCAL = 1;
    private static final int RC_KEY_CAUSAL_MAJORITY = 2;
    private static final int RC_KEY_LINEARIZABLE_ALL = 3;

    private static final HashMap<Integer, Double> token_costs = new HashMap<>() {{
        put(RC_KEY_EVENTUAL_ALL, 1.0);
        put(RC_KEY_CAUSAL_LOCAL, 1.54);
        put(RC_KEY_CAUSAL_MAJORITY, 2.0);
        put(RC_KEY_LINEARIZABLE_ALL, 10.0);
    }};

    private int getReadLatencyKey(ReadConcern readConcern, ReadLevel readLevel) {
        if (readConcern == ReadConcern.CAUSAL) {
            return readLevel == ReadLevel.MAJORITY ? RC_KEY_CAUSAL_MAJORITY : RC_KEY_CAUSAL_LOCAL;
        }
        if (readConcern == ReadConcern.LINEARIZABLE) {
            return RC_KEY_LINEARIZABLE_ALL;
        }
        return RC_KEY_EVENTUAL_ALL;
    }

    private ReadConcern readConcernFromKey(int key) {
        if (key == RC_KEY_LINEARIZABLE_ALL) {
            return ReadConcern.LINEARIZABLE;
        }
        if (key == RC_KEY_CAUSAL_LOCAL || key == RC_KEY_CAUSAL_MAJORITY) {
            return ReadConcern.CAUSAL;
        }
        return ReadConcern.EVENTUAL;
    }

    private ReadLevel readLevelFromKey(int key, ReadLevel fallback) {
        if (key == RC_KEY_CAUSAL_MAJORITY || key == RC_KEY_LINEARIZABLE_ALL) {
            return ReadLevel.MAJORITY;
        }
        if (key == RC_KEY_CAUSAL_LOCAL) {
            return ReadLevel.LOCAL;
        }
        return fallback;
    }

    private int getNextReadKey(int fromKey, boolean isLeader) {
        if (isLeader) {
            if (fromKey == RC_KEY_EVENTUAL_ALL) return RC_KEY_CAUSAL_MAJORITY;
            if (fromKey == RC_KEY_CAUSAL_LOCAL) return RC_KEY_CAUSAL_MAJORITY;
            if (fromKey == RC_KEY_CAUSAL_MAJORITY) return RC_KEY_LINEARIZABLE_ALL;
            return -1;
        }
        if (fromKey == RC_KEY_EVENTUAL_ALL) return RC_KEY_CAUSAL_LOCAL;
        if (fromKey == RC_KEY_CAUSAL_LOCAL) return RC_KEY_CAUSAL_MAJORITY;
        return -1;
    }

    private double getReadLatencyByKey(HashMap<Integer, Double> readLatencyByKey, int key) {
        return readLatencyByKey.getOrDefault(key, Double.MAX_VALUE);
    }

    private double getReadTokenCostByKey(int key) {
        return token_costs.getOrDefault(key, 0.1);
    }

    private double getReadTokenCost(ReadConcern readConcern, ReadLevel readLevel) {
        int key = getReadLatencyKey(readConcern, readLevel);
        return getReadTokenCostByKey(key);
    }

    /**
     * Check if system is still in warmup phase (first 7 seconds)
     */
    private boolean isInWarmupPhase() {
        return (System.currentTimeMillis() - systemStartTime) < WARMUP_DURATION_MS;
    }

    private boolean isInUpgradeWarmupPhase() {
        return (System.currentTimeMillis() - systemStartTime) < UPGRADE_WARMUP_MS;
    }


    /**
     * Get max TPS for a given write concern level
     * During warmup phase (first 7 seconds): use only MIN_HASH_MAP values
     * After warmup: use max of MIN_HASH_MAP and wcTpsMap (measured values)
     */
    private double getMaxTPS(int writeConcern, HashMap<Integer, Double> wcTpsMap) {
        if (isInWarmupPhase()) {
            // During warmup, only use MIN_HASH_MAP (conservative estimates)
            return MIN_HASH_MAP.getOrDefault(writeConcern, 0.0);
        }
        // After warmup, use the maximum of measured and minimum expected
        return wcTpsMap.get(writeConcern); 
    }

    private double getMaxLatency(int writeConcern, HashMap<Integer, Double> wcLatencyMap) {
        if(isInWarmupPhase()) {
            return 200; 
        }
        return wcLatencyMap.get(writeConcern);
    }

    /**
     * Calculate average TPS after adding batch
     * Formula: (currentTPS + sum of maxTPS[wc_i]) / (1 + batchSize)
     */
    private double calculateAvgTPS(double currentTPS, int[] consistencyLevels, HashMap<Integer, Double> wcTpsMap) {
        double sum = currentTPS;
        int count = 0;
        for (int wc : consistencyLevels) {
            if (wc > 0) {
                sum += getMaxTPS(wc, wcTpsMap);
                count++;
            }
        }
        return (count == 0) ? currentTPS : sum / (1 + count);
    }
   /**
     * Calculate average Latency after adding batch
     * Formula: (currentLatency + sum of maxLatency[wc_i]) / (1 + batchSize)
     */
    private double calculateAvgLatency(double currentLatency, int[] consistencyLevels, HashMap<Integer, Double> wcLatencyMap) {
        double sum = currentLatency;
        int count = 0;
        for (int wc : consistencyLevels) {
            if (wc > 0) {
                sum += getMaxLatency(wc, wcLatencyMap);
                count++;
            }
        }
        return (count == 0) ? currentLatency : sum / (1 + count);
    }

    /**
     * Overloaded: Calculate average Latency using ConsistencyAssignment[]
     */
    private double calculateAvgLatency(double currentLatency, ConsistencyAssignment[] assignments,
                                        HashMap<Integer, Double> wcLatencyMap,
                                        HashMap<Integer, Double> readLatencyByKey) {
        double sum = currentLatency;
        int count = 0;
        for (ConsistencyAssignment a : assignments) {
            if (!a.isDeferred()) {
                if (a.isReadOnly) {
                    int key = getReadLatencyKey(a.readConcern, a.readLevel);
                    sum += getReadLatencyByKey(readLatencyByKey, key);
                } else {
                    sum += getMaxLatency(a.writeConcern, wcLatencyMap);
                }
                count++;
            }
        }
        return (count == 0) ? currentLatency : sum / (1 + count);
    }

    /**
     * Process transactions with TPS-based heuristic.
     * 
     * Algorithm:
     * 1. Assign all transactions their minimum consistency
     * 2. Calculate: avgTPS = (currentTPS + Σ maxTPS[wc_i]) / (1 + batchSize)
     * 3. If avgTPS >= minTPS → execute all
     * 4. If avgTPS >= minTPS * 1.15 → can upgrade transactions
     * 5. Upgrade greedily while avgTPS stays >= minTPS * 1.10
     * 6. If avgTPS < minTPS → process from weakest first until avgTPS >= minTPS
     */

    public ProcessResult processWithTPSHeuristic(
            List<TransactionOption> batch,
            double currentTPS,
            HashMap<Integer, Double> wcTpsMap,
            Set<String> backLogTransactions) {

        int n = batch.size();
        double finalBatchAvgTps;

        if (n == 0) {
            return new ProcessResult(new ArrayList<>(), 0, 0, 0);
        }

        int majority = (NUM_OF_SERVERS / 2) + 1;
        int[] consistencyLevels = new int[n];
        double profit = 0;
        int transactionsUpgraded = 0;

        // Step 1: Initialize all at minimum consistency
        for (int i = 0; i < n; i++) {
            consistencyLevels[i] = batch.get(i).minRequiredConsistency;
        }

        // Step 2: Calculate avgTPS with all transactions at min consistency
        double avgTPS = calculateAvgTPS(currentTPS, consistencyLevels, wcTpsMap);

        System.out.printf("[TPS Heuristic] currentTPS=%.0f | avgTPS=%.0f | minTPS=%.0f%n", 
                currentTPS, avgTPS, MIN_TPS);
        
        // Print individual writeConcern throughputs
        System.out.print("[WriteConcern TPS] ");
        for (int wc = 1; wc <= majority; wc++) {
            System.out.printf("W:%d=%.2f TPS | ", wc, wcTpsMap.get(wc));
        }
        System.out.println();

        // Step 3: Check if we can execute all
        if (avgTPS >= MIN_TPS) {
            // Execute all transactions
            for (int i = 0; i < n; i++) {
                profit += batch.get(i).baseProfit;
            }

            // Step 4: Check if we can upgrade (need 15% headroom and no backlog)
                if (avgTPS >= MIN_TPS * UPGRADE_THRESHOLD
                    && backLogTransactions.isEmpty()
                    && currentTPS > MIN_TPS
                    && !isInUpgradeWarmupPhase()) {
                
                // Build priority queue for upgrades (best profit/tps-cost ratio first)
                PriorityQueue<UpgradeOption> pq = new PriorityQueue<>(
                        (a, b) -> Double.compare(b.ratio, a.ratio)
                );

                for (int i = 0; i < n; i++) {
                    int currentWC = consistencyLevels[i];
                    if (currentWC < majority) {
                        TransactionOption tx = batch.get(i);
                        double tpsDrop = getMaxTPS(currentWC, wcTpsMap) - getMaxTPS(currentWC + 1, wcTpsMap);
                        double nextProfit = (currentWC + 1 == majority)
                                ? tx.extraMajorityProfit
                                : tx.extraIntermediateProfit;
                        double ratio = (tpsDrop > EPS) ? nextProfit / tpsDrop : Double.MAX_VALUE;
                        pq.add(new UpgradeOption(i, currentWC, currentWC + 1, tpsDrop, nextProfit, ratio));
                    }
                }

                // Step 5: Upgrade while avgTPS stays >= minTPS * 1.10
                while (!pq.isEmpty()) {
                    UpgradeOption opt = pq.poll();

                    if (opt.fromWC != consistencyLevels[opt.txIndex]) continue;

                    // Simulate upgrade

                    // we want to upgrade the transactions consistency level to opt.toWC
                    avgTPS = (avgTPS * (n + 1) - getMaxTPS(opt.fromWC, wcTpsMap) + getMaxTPS(opt.toWC, wcTpsMap)) / (n + 1);
                    // int[] tempLevels = consistencyLevels.clone();
                    // tempLevels[opt.txIndex] = opt.toWC;
                    // double newAvgTPS = calculateAvgTPS(currentTPS, tempLevels, wcTpsMap);

                    if (avgTPS >= MIN_TPS * UPGRADE_FLOOR) {
                        // Safe to upgrade
                        consistencyLevels[opt.txIndex] = opt.toWC;
                        profit += opt.additionalProfit;
                        transactionsUpgraded++;

                        // Add next upgrade level if possible
                        if (opt.toWC < majority) {
                            TransactionOption tx = batch.get(opt.txIndex);
                            double tpsDrop = getMaxTPS(opt.toWC, wcTpsMap) - getMaxTPS(opt.toWC + 1, wcTpsMap);
                            double nextProfit = (opt.toWC + 1 == majority)
                                    ? tx.extraMajorityProfit
                                    : tx.extraIntermediateProfit;
                            double ratio = (tpsDrop > EPS) ? nextProfit / tpsDrop : Double.MAX_VALUE;
                            pq.add(new UpgradeOption(opt.txIndex, opt.toWC, opt.toWC + 1, tpsDrop, nextProfit, ratio));
                        }
                    }
                }
            }
            logFinalBatchAvgTps(avgTPS);
            // Build result
            BuildResult buildResult = buildResultMessages(batch, consistencyLevels, backLogTransactions);

            
            HashMap<Integer, Integer> wcMix = countByWriteConcern(consistencyLevels);
            System.out.printf("[TPS Heuristic] EXECUTED ALL | Mix=%s | Upgraded=%d | Backlog=%d | FinalAvgTPS=%.0f%n",
                    wcMix, transactionsUpgraded, backLogTransactions.size(), avgTPS);

            return new ProcessResult(buildResult.executed, n, profit, transactionsUpgraded, buildResult.deferred);
        }
        // Step 6: avgTPS < minTPS → process from weakest first
        else {
            // Sort by: 1) retryCount DESC (prioritize retried transactions)
            //          2) minRequiredConsistency ASC (weakest first)
            List<Integer> indices = new ArrayList<>();
            for (int i = 0; i < n; i++) indices.add(i);
            indices.sort((a, b) -> {
                TransactionOption txA = batch.get(a);
                TransactionOption txB = batch.get(b);
                // First: higher retry count has priority
                if (txA.retryCount != txB.retryCount) {
                    return Integer.compare(txB.retryCount, txA.retryCount); // DESC
                }
                // Second: lower consistency (W:1 before W:2)
                return Integer.compare(txA.minRequiredConsistency, txB.minRequiredConsistency);
            });

            // Reset consistency levels
            for (int i = 0; i < n; i++) consistencyLevels[i] = 0;

            // Minimum transactions to process (80% of batch)
            int minToProcess = Math.max(1, (int) (n * 0.7));

            int processed = 0;
            double tpsSum = currentTPS;
            for (int idx : indices) {
                TransactionOption tx = batch.get(idx);
                
                // Force execute transactions that have been deferred too many times (retryCount >= 2)
                boolean mustExecute = tx.retryCount >= 2;

                // System.out.printf("[TPS Heuristic] Considering Tx ID=%s | MinWC=%d | RetryCount=%d | MustExecute=%b%n",
                //         tx.id, tx.minRequiredConsistency, tx.retryCount, mustExecute);
                
                // Try adding this transaction
                consistencyLevels[idx] = tx.minRequiredConsistency;
                tpsSum += getMaxTPS(tx.minRequiredConsistency, wcTpsMap);
                double newAvgTPS = tpsSum / (processed + 1);

                // Execute if: forced (retry >= 2), below minimum threshold, or TPS allows
                if (mustExecute || processed < minToProcess || newAvgTPS >= MIN_TPS) {
                    // Must process (retried too many times / below minimum) or can process (TPS allows)
                    profit += tx.baseProfit;
                    processed++;
                } else {
                    // Would drop below minTPS and we've hit minimum, skip it
                    consistencyLevels[idx] = 0;
                }
            }

            finalBatchAvgTps = tpsSum / (processed + 1);

            BuildResult buildResult = buildResultMessages(batch, consistencyLevels, backLogTransactions);

            logFinalBatchAvgTps(finalBatchAvgTps);
            System.out.printf("[TPS Heuristic] UNDER PRESSURE | Processed=%d/%d | Deferred=%d | Backlog=%d | FinalAvgTPS=%.0f%n",
                    processed, n, buildResult.deferred.size(), backLogTransactions.size(), finalBatchAvgTps);

            return new ProcessResult(buildResult.executed, processed, profit, 0, buildResult.deferred);
        }
    }


    public ProcessResult processWithLatencyHeuristic(
        List<TransactionOption> batch,
        double currentLatency,
        HashMap<Integer, Double> wcLatencyMap,
        HashMap<Integer, Double> wcTpsMap,
        int incomingRateOfTransactions,
        Set<String> backLogTransactions, boolean isBackLogIncreasing) {

        int n = batch.size();
        double finalBatchAvgLatency;

        if (n == 0) {
            return new ProcessResult(new ArrayList<>(), 0, 0, 0);
        }

        int majority = (NUM_OF_SERVERS / 2) + 1;
        int[] consistencyLevels = new int[n];
        double profit = 0;
        int transactionsUpgraded = 0;

        // Step 1: Initialize all at minimum consistency
        for (int i = 0; i < n; i++) {
            consistencyLevels[i] = batch.get(i).minRequiredConsistency;
        }

        // Step 2: Calculate avgLatency with all transactions at min consistency
        double avgLatency = calculateAvgLatency(currentLatency, consistencyLevels, wcLatencyMap);

        System.out.printf("[LATENCY Heuristic] currentLatency=%.2f | avgLatency=%.2f | maxLatency=%.2f\n",
                currentLatency, avgLatency, MAX_LATENCY);

        // Print individual writeConcern latencies
        System.out.print("[WriteConcern Latency] ");
        for (int wc = 1; wc <= majority; wc++) {
            System.out.printf("W:%d=%.2f ms | ", wc, wcLatencyMap.get(wc));
        }
        System.out.println();

        // Step 3: Check if we can execute all
        if (avgLatency <= MAX_LATENCY) {
            // Execute all transactions
            for (int i = 0; i < n; i++) {
                profit += batch.get(i).baseProfit;
            }

            // Step 4: Check if we can upgrade (need 15% headroom and no backlog)
            // ** very important **
            // we can add that if tps falls then we expect latency to rise and stop the upgrades, or we can add a check if the current tps of the majority is lower than the load then we can stop the upgrades for sure
            
            boolean canUpgrade = false;
                if (avgLatency <= MAX_LATENCY * UPGRADE_LATENCY_THRESHOLD
                    && backLogTransactions.isEmpty()
                    && currentLatency < MAX_LATENCY
                    && !isBackLogIncreasing
                    && !isInUpgradeWarmupPhase()) {
                // Build priority queue for upgrades (best profit/latency-cost ratio first)
                PriorityQueue<UpgradeOption> pq = new PriorityQueue<>(
                        (a, b) -> Double.compare(b.ratio, a.ratio)
                );

                for (int i = 0; i < n; i++) {
                    int currentWC = consistencyLevels[i];
                    if (currentWC < majority) {
                        TransactionOption tx = batch.get(i);
                        double latencyInc = getMaxLatency(currentWC + 1, wcLatencyMap) - getMaxLatency(currentWC, wcLatencyMap);
                        double nextProfit = (currentWC + 1 == majority)
                                ? tx.extraMajorityProfit
                                : tx.extraIntermediateProfit;
                        double ratio = (latencyInc > EPS) ? nextProfit / latencyInc : Double.MAX_VALUE;
                        pq.add(new UpgradeOption(i, currentWC, currentWC + 1, latencyInc, nextProfit, ratio));
                    }
                }

                // Step 5: Upgrade while avgLatency stays <= maxLatency * 1.10
                while (!pq.isEmpty()) {
                    UpgradeOption opt = pq.poll();

                    if (opt.fromWC != consistencyLevels[opt.txIndex]) continue;

                    // Simulate upgrade
                    avgLatency = (avgLatency * (n + 1) - getMaxLatency(opt.fromWC, wcLatencyMap) + getMaxLatency(opt.toWC, wcLatencyMap)) / (n + 1);

                    if (avgLatency <= MAX_LATENCY * UPGRADE_LATENCY_FLOOR) {
                        // Safe to upgrade
                        consistencyLevels[opt.txIndex] = opt.toWC;
                        profit += opt.additionalProfit;
                        transactionsUpgraded++;
                        // Add next upgrade level if possible
                        if (opt.toWC < majority) {
                            TransactionOption tx = batch.get(opt.txIndex);
                            double latencyInc = getMaxLatency(opt.toWC + 1, wcLatencyMap) - getMaxLatency(opt.toWC, wcLatencyMap);
                            double nextProfit = (opt.toWC + 1 == majority)
                                    ? tx.extraMajorityProfit
                                    : tx.extraIntermediateProfit;
                            double ratio = (latencyInc > EPS) ? nextProfit / latencyInc : Double.MAX_VALUE;
                            pq.add(new UpgradeOption(opt.txIndex, opt.toWC, opt.toWC + 1, latencyInc, nextProfit, ratio));
                        }
                    }
                }
            }
            // logFinalBatchAvgLatency(avgLatency);
            // Build result
            BuildResult buildResult = buildResultMessages(batch, consistencyLevels, backLogTransactions);

            HashMap<Integer, Integer> wcMix = countByWriteConcern(consistencyLevels);
            System.out.printf("[LATENCY Heuristic] EXECUTED ALL | Mix=%s | Upgraded=%d | Backlog=%d | FinalAvgLatency=%.2f\n",
                    wcMix, transactionsUpgraded, backLogTransactions.size(), avgLatency);

            return new ProcessResult(buildResult.executed, n, profit, transactionsUpgraded, buildResult.deferred);
        }
        // Step 6: avgLatency > maxLatency → process from weakest first
        else {
            // Sort by: 1) retryCount DESC (prioritize retried transactions)
            //          2) minRequiredConsistency ASC (weakest first)
            List<Integer> indices = new ArrayList<>();
            for (int i = 0; i < n; i++) indices.add(i);
            indices.sort((a, b) -> {
                TransactionOption txA = batch.get(a);
                TransactionOption txB = batch.get(b);
                // First: higher retry count has priority
                if (txA.retryCount != txB.retryCount) {
                    return Integer.compare(txB.retryCount, txA.retryCount); // DESC
                }
                // Second: lower consistency (W:1 before W:2)
                return Integer.compare(txA.minRequiredConsistency, txB.minRequiredConsistency);
            });

            // Reset consistency levels
            for (int i = 0; i < n; i++) consistencyLevels[i] = 0;

            // Minimum transactions to process (70% of batch)
            int minToProcess = Math.max(1, (int) (n * 0.7));

            int processed = 0;
            double latencySum = currentLatency;
            for (int idx : indices) {
                TransactionOption tx = batch.get(idx);

                // Force execute transactions that have been deferred too many times (retryCount >= 2)
                boolean mustExecute = tx.retryCount >= 2;

                // Try adding this transaction
                consistencyLevels[idx] = tx.minRequiredConsistency;
                latencySum += getMaxLatency(tx.minRequiredConsistency, wcLatencyMap);
                double newAvgLatency = latencySum / (processed + 1);

                // Execute if: forced (retry >= 2), below minimum threshold, or latency allows
                if (mustExecute || processed < minToProcess || newAvgLatency <= MAX_LATENCY || wcTpsMap.get((NUM_OF_SERVERS) / 2 + 1) > MIN_TPS_OF_MAJORITY) {
                    profit += tx.baseProfit;
                    processed++;
                } else {
                    consistencyLevels[idx] = 0;
                }
            }

            finalBatchAvgLatency = latencySum / (processed + 1);

            BuildResult buildResult = buildResultMessages(batch, consistencyLevels, backLogTransactions);

            // logFinalBatchAvgLatency(finalBatchAvgLatency);
            System.out.printf("[LATENCY Heuristic] UNDER PRESSURE | Processed=%d/%d | Deferred=%d | Backlog=%d | FinalAvgLatency=%.2f\n",
                    processed, n, buildResult.deferred.size(), backLogTransactions.size(), finalBatchAvgLatency);

            return new ProcessResult(buildResult.executed, processed, profit, 0, buildResult.deferred);
        }
    }


    public ProcessResult processWithLatencyApplicationBasedHeuristic(
    List<TransactionOption> batch,
    double currentLatency,
    HashMap<Integer, Double> wcLatencyMap,
    HashMap<Integer, Double> wcTpsMap,
    int incomingRateOfTransactions,
    Set<String> backLogTransactions, boolean isBackLogIncreasing, double currentTokens,
     HashMap<Integer, Double> readLatencyByKey, boolean isLeader,
     Queue<TransactionOption> deferredQueue) {
        return processWithLatencyApplicationBasedHeuristicInternal(
                batch,
                currentLatency,
                wcLatencyMap,
                wcTpsMap,
                incomingRateOfTransactions,
                backLogTransactions,
                isBackLogIncreasing,
                currentTokens,
                readLatencyByKey,
                isLeader,
                deferredQueue,
                true);
    }

    public ProcessResult processWithLatencyApplicationBasedHeuristicNoPressure(
    List<TransactionOption> batch,
    double currentLatency,
    HashMap<Integer, Double> wcLatencyMap,
    HashMap<Integer, Double> wcTpsMap,
    int incomingRateOfTransactions,
    Set<String> backLogTransactions, boolean isBackLogIncreasing, double currentTokens,
    HashMap<Integer, Double> readLatencyByKey, boolean isLeader,
    Queue<TransactionOption> deferredQueue) {
        return processWithLatencyApplicationBasedHeuristicInternal(
                batch,
                currentLatency,
                wcLatencyMap,
                wcTpsMap,
                incomingRateOfTransactions,
                backLogTransactions,
                isBackLogIncreasing,
                currentTokens,
                readLatencyByKey,
                isLeader,
                deferredQueue,
                false);
    }

    private ProcessResult processWithLatencyApplicationBasedHeuristicInternal(
    List<TransactionOption> batch,
    double currentLatency,
    HashMap<Integer, Double> wcLatencyMap,
    HashMap<Integer, Double> wcTpsMap,
    int incomingRateOfTransactions,
    Set<String> backLogTransactions, boolean isBackLogIncreasing, double currentTokens,
     HashMap<Integer, Double> readLatencyByKey, boolean isLeader,
     Queue<TransactionOption> deferredQueue,
     boolean pressureModeEnabled) {

    int n = batch.size();

    if (n == 0) {
        return new ProcessResult(new ArrayList<>(), 0, 0, 0);
    }

    int majority = (NUM_OF_SERVERS / 2) + 1;
    ConsistencyAssignment[] assignments = new ConsistencyAssignment[n];
    double profit = 0;
    int transactionsUpgraded = 0;

    // Step 1: Initialize all at minimum consistency
    for (int i = 0; i < n; i++) {
        TransactionOption txInit = batch.get(i);
        assignments[i] = new ConsistencyAssignment(
                txInit.minRequiredConsistency,
                txInit.readConcern,
                txInit.readLevel,
                txInit.isReadOnly);
    }

    // Step 2: Single pass — build count map, profit lookup maps, and batch index
    // Write maps: appId -> (wc -> count/indices)
    // Read maps:  appId -> (readConcern# -> count/indices)
    HashMap<Integer, HashMap<Integer, Integer>> appWcCount = new HashMap<>();
    HashMap<Integer, Double> appIntermediateProfitMap = new HashMap<>();
    HashMap<Integer, Double> appMajorityProfitMap = new HashMap<>();
    HashMap<Integer, HashMap<Integer, List<Integer>>> appWcBatchIndex = new HashMap<>();
    HashMap<Integer, Double> appBaseProfitMap = new HashMap<>();
    HashMap<Integer, HashMap<Integer,HashMap<Integer,Integer>>> wcUpgradesMap = new HashMap<>();

    // Read transaction maps — keyed by read-latency key
    HashMap<Integer, HashMap<Integer, Integer>> appReadConcernCount = new HashMap<>();
    HashMap<Integer, HashMap<Integer, List<Integer>>> appReadConcernBatchIndex = new HashMap<>();
    HashMap<Integer, HashMap<Integer, HashMap<Integer, Integer>>> readConcernUpgradesMap = new HashMap<>();

    for (int i = 0; i < n; i++) {
        TransactionOption tx = batch.get(i);
        int appId = tx.applicationId;

        // Shared profit maps
        appIntermediateProfitMap.putIfAbsent(appId, tx.extraIntermediateProfit);
        appMajorityProfitMap.putIfAbsent(appId, tx.extraMajorityProfit);
        appBaseProfitMap.putIfAbsent(appId, tx.baseProfit);

        if (tx.isReadOnly) {
            // Read transaction — group by key(readConcern, readLevel)
            int rc = getReadLatencyKey(tx.readConcern, tx.readLevel);
            appReadConcernCount
                .computeIfAbsent(appId, k -> new HashMap<>())
                .merge(rc, 1, Integer::sum);
            appReadConcernBatchIndex
                .computeIfAbsent(appId, k -> new HashMap<>())
                .computeIfAbsent(rc, k -> new ArrayList<>())
                .add(i);
            readConcernUpgradesMap
                .computeIfAbsent(appId, k -> new HashMap<>())
                .computeIfAbsent(rc, k -> new HashMap<>());
        } else {
            // Write transaction — group by writeConcern
            int wc = tx.minRequiredConsistency;
            appWcCount
                .computeIfAbsent(appId, k -> new HashMap<>())
                .merge(wc, 1, Integer::sum);
            appWcBatchIndex
                .computeIfAbsent(appId, k -> new HashMap<>())
                .computeIfAbsent(wc, k -> new ArrayList<>())
                .add(i);
            wcUpgradesMap
                .computeIfAbsent(appId, k -> new HashMap<>())
                .computeIfAbsent(wc, k -> new HashMap<>());
        }
    }

    // System.out.println("App-WC Count Map:");
    // System.out.println(appWcCount);
    // System.out.println("App-ReadConcern Count Map:");
    // System.out.println(appReadConcernCount);

    // Step 3: Calculate avgLatency with all transactions at min consistency
    double avgLatency = calculateAvgLatency(currentLatency, assignments, wcLatencyMap, readLatencyByKey);

    // System.out.printf("[APP Heuristic] currentLatency=%.2f | avgLatency=%.2f | maxLatency=%.2f\n",
    //         currentLatency, avgLatency, MAX_LATENCY);

    // System.out.print("[WriteConcern Latency] ");
    // for (int wc = 1; wc <= majority; wc++) {
    //     System.out.printf("W:%d=%.2f ms | ", wc, wcLatencyMap.get(wc));
    // }
    // System.out.println();

    // Step 4: Check if we can execute all.
    // In no-pressure mode, base token cost is assumed to be already paid.
    double totalTokenCost = calculateTotalTokenCost(batch, assignments);
    boolean skipBaseTokenGate = !pressureModeEnabled;
    MAX_LATENCY = isLeader ?  50 : 50;
    if (avgLatency <= MAX_LATENCY && (skipBaseTokenGate || totalTokenCost <= currentTokens)) {

        for (int i = 0; i < n; i++) {
            profit += batch.get(i).baseProfit;
        }

        // Tracks token usage baseline + upgrades.
        // In no-pressure mode, baseline is 0 (base cost already paid).
        double tokensUsedSoFar = skipBaseTokenGate ? 0.0 : totalTokenCost;

        // Step 5: Upgrade phase
        if (avgLatency <= MAX_LATENCY * UPGRADE_LATENCY_THRESHOLD
                && deferredQueue.isEmpty()
                && currentLatency < MAX_LATENCY
            && isBackLogIncreasing == false
            && !isInUpgradeWarmupPhase()) {

            // PQ: unified for write-WC upgrades and read-RC upgrades
            // ranked by per-transaction ratio (profit per unit cost)
            PriorityQueue<AppUpgradeOption> pq = new PriorityQueue<>(
                    (a, b) -> Double.compare(b.ratio, a.ratio)
            );

            // === Seed write upgrades: for every app, for every WC level that can go higher ===
            for (Map.Entry<Integer, HashMap<Integer, Integer>> appEntry : appWcCount.entrySet()) {
                int appId = appEntry.getKey();
                for (Map.Entry<Integer, Integer> wcEntry : appEntry.getValue().entrySet()) {
                    int fromWC = wcEntry.getKey();
                    int count = wcEntry.getValue();
                    if (fromWC >= majority || count == 0) continue;

                    int toWC = fromWC + 1;
                    double latencyIncPerTx = getMaxLatency(toWC, wcLatencyMap) - getMaxLatency(fromWC, wcLatencyMap);

                    double perTxProfit = (toWC == majority)
                            ? appMajorityProfitMap.get(appId)
                            : appIntermediateProfitMap.get(appId);

                    double ratio = (latencyIncPerTx > EPS) ? perTxProfit / latencyIncPerTx : Double.MAX_VALUE;
                    pq.add(new AppUpgradeOption(appId, fromWC, toWC, count, ratio, fromWC,
                            false, 0.0)); // isReadUpgrade=false
                }
            }

                // === Seed read upgrades using key chain ===

                for (Map.Entry<Integer, HashMap<Integer, Integer>> appEntry : appReadConcernCount.entrySet()) {
                int appId = appEntry.getKey();
                for (Map.Entry<Integer, Integer> keyEntry : appEntry.getValue().entrySet()) {
                    int fromKey = keyEntry.getKey();
                    int count = keyEntry.getValue();
                    if (count <= 0) continue;

                    int toKey = getNextReadKey(fromKey, isLeader);
                    if (toKey == -1) continue;

                    double fromLatency = getReadLatencyByKey(readLatencyByKey, fromKey);
                    double toLatency = getReadLatencyByKey(readLatencyByKey, toKey);
                    if (toLatency > MAX_LATENCY) continue;
                    double readLatencyIncPerTx = toLatency - fromLatency;

                    ReadConcern fromConcern = readConcernFromKey(fromKey);
                    ReadConcern toConcern = readConcernFromKey(toKey);
                    double tokenCostIncrease = getReadTokenCostByKey(toKey)
                        - getReadTokenCostByKey(fromKey);

                    double perTxProfit = (toKey == RC_KEY_LINEARIZABLE_ALL)
                        ? appMajorityProfitMap.getOrDefault(appId, 0.0)
                        : appIntermediateProfitMap.getOrDefault(appId, 0.0);
                    double ratio = (readLatencyIncPerTx > EPS)
                        ? perTxProfit / readLatencyIncPerTx : Double.MAX_VALUE;

                    pq.add(new AppUpgradeOption(appId, fromKey, toKey, count,
                        ratio, fromKey, true, tokenCostIncrease));
                }
            }

            // Step 6: Unified greedy upgrade loop
            while (!pq.isEmpty()) {
                AppUpgradeOption opt = pq.poll();

                if (opt.isReadUpgrade) {
                        // --- Read upgrade (key(from) -> key(to)) ---
                    int currentCount = appReadConcernCount
                            .getOrDefault(opt.appId, new HashMap<>())
                            .getOrDefault(opt.fromWC, 0);
                    if (currentCount == 0) continue;

                    // Check token budget
                    double remainingTokenBudget = currentTokens - tokensUsedSoFar;
                    int maxByTokens = (opt.tokenCostPerTx > EPS)
                            ? (int) Math.floor(remainingTokenBudget / opt.tokenCostPerTx)
                            : currentCount;

                    // Check latency headroom (same constraint as write upgrades)
                        double readLatencyInc = getReadLatencyByKey(readLatencyByKey, opt.toWC)
                            - getReadLatencyByKey(readLatencyByKey, opt.fromWC);
                    double remainingHeadroom = (MAX_LATENCY * UPGRADE_LATENCY_FLOOR - avgLatency) * (n + 1);
                    int maxByLatency = (readLatencyInc > EPS)
                            ? (int) Math.floor(remainingHeadroom / readLatencyInc)
                            : currentCount;

                    int toUpgrade = Math.min(currentCount, Math.min(maxByTokens, maxByLatency));
                    if (toUpgrade <= 0) continue;

                    tokensUsedSoFar += opt.tokenCostPerTx * toUpgrade;
                    avgLatency += (readLatencyInc * toUpgrade) / (n + 1);
                    appReadConcernCount.get(opt.appId).put(opt.fromWC, currentCount - toUpgrade);
                    appReadConcernCount.get(opt.appId).merge(opt.toWC, toUpgrade, Integer::sum);
                    transactionsUpgraded += toUpgrade;

                    readConcernUpgradesMap.get(opt.appId).get(opt.originalWc)
                            .put(opt.toWC, readConcernUpgradesMap.get(opt.appId)
                                    .get(opt.originalWc).getOrDefault(opt.toWC, 0) + toUpgrade);
                        if (opt.fromWC != opt.originalWc) {
                        readConcernUpgradesMap.get(opt.appId).get(opt.originalWc)
                            .put(opt.fromWC, readConcernUpgradesMap.get(opt.appId)
                                .get(opt.originalWc).getOrDefault(opt.fromWC, 0) - toUpgrade);
                        }

                        double perTxProfit = (opt.toWC == RC_KEY_LINEARIZABLE_ALL)
                            ? appMajorityProfitMap.getOrDefault(opt.appId, 0.0)
                            : appIntermediateProfitMap.getOrDefault(opt.appId, 0.0);
                    profit += toUpgrade * perTxProfit;

                        int nextKey = getNextReadKey(opt.toWC, isLeader);
                        int nowAtToKey = appReadConcernCount.get(opt.appId).getOrDefault(opt.toWC, 0);
                        if (nextKey != -1) {
                        double nextFromLatency = getReadLatencyByKey(readLatencyByKey, opt.toWC);
                        double nextToLatency = getReadLatencyByKey(readLatencyByKey, nextKey);
                        if (nextToLatency <= MAX_LATENCY) {
                            ReadConcern fromConcern = readConcernFromKey(opt.toWC);
                            ReadConcern toConcern = readConcernFromKey(nextKey);
                            double nextTokenCostIncrease = getReadTokenCostByKey(nextKey)
                                - getReadTokenCostByKey(opt.toWC);
                            double nextLatencyInc = nextToLatency - nextFromLatency;
                            double nextProfit = (nextKey == RC_KEY_LINEARIZABLE_ALL)
                                ? appMajorityProfitMap.getOrDefault(opt.appId, 0.0)
                                : appIntermediateProfitMap.getOrDefault(opt.appId, 0.0);
                            double nextRatio = (nextLatencyInc > EPS)
                                ? nextProfit / nextLatencyInc : Double.MAX_VALUE;
                            pq.add(new AppUpgradeOption(opt.appId, opt.toWC, nextKey, nowAtToKey,
                                nextRatio, opt.originalWc, true, nextTokenCostIncrease));
                        }
                        }

                } else {
                    // --- Write upgrade (WC level bump) ---
                    int currentCount = appWcCount.getOrDefault(opt.appId, new HashMap<>()).getOrDefault(opt.fromWC, 0);
                    if (currentCount == 0) continue;

                    double latencyIncPerTx = getMaxLatency(opt.toWC, wcLatencyMap) - getMaxLatency(opt.fromWC, wcLatencyMap);
                    double remainingHeadroom = (MAX_LATENCY * UPGRADE_LATENCY_FLOOR - avgLatency) * (n + 1);

                    int maxAffordable = (latencyIncPerTx > EPS)
                            ? (int) Math.floor(remainingHeadroom / latencyIncPerTx)
                            : currentCount;

                    int toUpgrade = Math.min(currentCount, maxAffordable);
                    if (toUpgrade <= 0) continue;

                    avgLatency += (latencyIncPerTx * toUpgrade) / (n + 1);
                    appWcCount.get(opt.appId).put(opt.fromWC, currentCount - toUpgrade);
                    appWcCount.get(opt.appId).merge(opt.toWC, toUpgrade, Integer::sum);
                    transactionsUpgraded += toUpgrade;

                    profit += toUpgrade * ((opt.toWC == majority)
                            ? appMajorityProfitMap.get(opt.appId)
                            : appIntermediateProfitMap.get(opt.appId));

                    int originalWc = opt.originalWc;
                    wcUpgradesMap.get(opt.appId).get(originalWc).put(opt.toWC,
                            wcUpgradesMap.get(opt.appId).get(originalWc).getOrDefault(opt.toWC, 0) + toUpgrade);
                    if (opt.fromWC != originalWc) {
                        wcUpgradesMap.get(opt.appId).get(originalWc).put(opt.fromWC,
                                wcUpgradesMap.get(opt.appId).get(originalWc).getOrDefault(opt.fromWC, 0) - toUpgrade);
                    }

                    // Seed next write level if still below majority
                    if (opt.toWC < majority) {
                        int nowAtToWC = appWcCount.get(opt.appId).getOrDefault(opt.toWC, 0);
                        int nextWC = opt.toWC + 1;
                        double nextLatencyIncPerTx = getMaxLatency(nextWC, wcLatencyMap) - getMaxLatency(opt.toWC, wcLatencyMap);
                        double nextPerTxProfit = (nextWC == majority)
                                ? appMajorityProfitMap.get(opt.appId)
                                : appIntermediateProfitMap.get(opt.appId);
                        double nextRatio = (nextLatencyIncPerTx > EPS) ? nextPerTxProfit / nextLatencyIncPerTx : Double.MAX_VALUE;
                        pq.add(new AppUpgradeOption(opt.appId, opt.toWC, nextWC, nowAtToWC,
                                nextRatio, originalWc, false, 0.0));
                    }
                }
            }

            // System.out.println("Final App-WC Count Map After Upgrades:");
            // System.out.println(appWcCount);
            // System.out.println("Final App-ReadConcern Count Map After Upgrades:");
            // System.out.println(appReadConcernCount);

            // Assign write upgrades to individual write transactions
            for (int i = 0; i < n; i++) {
                TransactionOption tx = batch.get(i);
                if (tx.isReadOnly) continue;
                int appId = tx.applicationId;
                int minWC = tx.minRequiredConsistency;
                HashMap<Integer, HashMap<Integer, Integer>> wcUpgrades = wcUpgradesMap.getOrDefault(appId, new HashMap<>());

                int assignedWC = minWC;
                HashMap<Integer, Integer> upgrades = wcUpgrades.getOrDefault(minWC, new HashMap<>());
                for (int wc = majority; wc >= minWC; wc--) {
                    int remaining = upgrades.getOrDefault(wc, 0);
                    if (remaining > 0) {
                        assignedWC = wc;
                        upgrades.put(wc, remaining - 1);
                        break;
                    }
                }
                assignments[i].writeConcern = assignedWC;
            }

            // Assign read upgrades to individual read transactions
            for (int i = 0; i < n; i++) {
                TransactionOption tx = batch.get(i);
                if (!tx.isReadOnly) continue;
                int appId = tx.applicationId;
                int originalKey = getReadLatencyKey(tx.readConcern, tx.readLevel);
                HashMap<Integer, Integer> upgrades = readConcernUpgradesMap
                        .getOrDefault(appId, new HashMap<>())
                        .getOrDefault(originalKey, new HashMap<>());

                int assignedKey = originalKey;
                for (int key = RC_KEY_LINEARIZABLE_ALL; key >= RC_KEY_EVENTUAL_ALL; key--) {
                    int remaining = upgrades.getOrDefault(key, 0);
                    if (remaining > 0) {
                        assignedKey = key;
                        upgrades.put(key, remaining - 1);
                        break;
                    }
                }

                assignments[i].readConcern = readConcernFromKey(assignedKey);
                assignments[i].readLevel = readLevelFromKey(assignedKey, tx.readLevel);
            }
        }

        // Calculate final token usage after all upgrades.
        // In no-pressure mode, return only differential upgrade cost tracked in tokensUsedSoFar.
        double finalTokenCost = calculateTotalTokenCost(batch, assignments);
        double tokensUsedForResult = pressureModeEnabled
            ? finalTokenCost
            : tokensUsedSoFar;

        BuildResult buildResult = buildResultMessages(batch, assignments, backLogTransactions, deferredQueue);
        HashMap<Integer, Integer> wcMix = countByWriteConcern(assignments);
        // System.out.printf("[APP Heuristic] EXECUTED ALL | Mix=%s | Upgraded=%d | Backlog=%d | FinalAvgLatency=%.2f | TokensUsed=%.2f\n",
        //         wcMix, transactionsUpgraded, deferredQueue.size(), avgLatency, finalTokenCost);

        return new ProcessResult(buildResult.executed, tokensUsedForResult, profit, transactionsUpgraded, buildResult.deferred);
    }

    // Step 8: avgLatency > maxLatency — pressure path, token-budget based
    else {
        if (!pressureModeEnabled) {
            for (int i = 0; i < n; i++) {
                profit += batch.get(i).baseProfit;
            }
            double tokensUsed = 0.0;
            BuildResult buildResult = buildResultMessages(batch, assignments, backLogTransactions, null);
            return new ProcessResult(buildResult.executed, tokensUsed, profit, 0, List.of());
        }

        // Reset all assignments to deferred
        for (int i = 0; i < n; i++) {
            if (assignments[i].isReadOnly) {
                assignments[i].readConcern = null;   // null = deferred for reads
                assignments[i].readLevel = null;
            } else {
                assignments[i].writeConcern = 0;     // 0 = deferred for writes
            }
        }

        int processed = 0;
        double tokensRemaining = currentTokens;

        // Collect all app IDs from both writes and reads
        Set<Integer> allAppIds = new HashSet<>(appWcCount.keySet());
        allAppIds.addAll(appReadConcernCount.keySet());
        List<Integer> appIdsDesc = new ArrayList<>(allAppIds);
        appIdsDesc.sort((a, b) -> {
            double pa = appBaseProfitMap.getOrDefault(a, 0.0);
            double pb = appBaseProfitMap.getOrDefault(b, 0.0);
            int byProfit = Double.compare(pb, pa); // higher profit first
            if (byProfit != 0) {
                return byProfit;
            }
            return Integer.compare(b, a); // deterministic fallback
        });


        // System.out.println("Backlog is "+ deferredQueue.size());

        // Build one deterministic execution order for pressure handling:
        // reads first by read key, then writes by write concern, both with appId DESC.
        List<Integer> pressureOrder = new ArrayList<>(n);
        for (int rc = RC_KEY_EVENTUAL_ALL; rc <= RC_KEY_LINEARIZABLE_ALL; rc++) {
            for (int appId : appIdsDesc) {
                List<Integer> idxList = appReadConcernBatchIndex
                        .getOrDefault(appId, new HashMap<>())
                        .get(rc);
                if (idxList != null && !idxList.isEmpty()) {
                    pressureOrder.addAll(idxList);
                }
            }
        }
        for (int wc = 1; wc <= majority; wc++) {
            for (int appId : appIdsDesc) {
                List<Integer> idxList = appWcBatchIndex
                        .getOrDefault(appId, new HashMap<>())
                        .get(wc);
                if (idxList != null && !idxList.isEmpty()) {
                    pressureOrder.addAll(idxList);
                }
            }
        }

        // Build retry-first plan once while preserving the original pressure order.
        List<Integer> retryOrder = new ArrayList<>(pressureOrder.size());
        List<Integer> regularOrder = new ArrayList<>(pressureOrder.size());
        for (int idx : pressureOrder) {
            TransactionOption tx = batch.get(idx);
            if (tx.retryCount >= 2) {
                retryOrder.add(idx);
            } else {
                regularOrder.add(idx);
            }
        }

        // Execute retries first.
        double[] tokensRemainingHolder = new double[] { tokensRemaining };
        double[] profitHolder = new double[] { profit };
        for (int idx : retryOrder) {
            if (tokensRemainingHolder[0] <= 0) {
                break;
            }
            processed += tryExecuteDeferredTransaction(batch, assignments, idx, tokensRemainingHolder, profitHolder);
        }

        // Execute remaining deferred transactions using the same deterministic order.
        int minToProcess = Math.max(1, (int) (n * 1));
        for (int idx : regularOrder) {
            if (tokensRemainingHolder[0] <= 0 || processed >= minToProcess) {
                break;
            }
            processed += tryExecuteDeferredTransaction(batch, assignments, idx, tokensRemainingHolder, profitHolder);
        }

        tokensRemaining = tokensRemainingHolder[0];
        profit = profitHolder[0];

        double tokensUsed = currentTokens - tokensRemaining;
        BuildResult buildResult = buildResultMessages(batch, assignments, backLogTransactions, deferredQueue);

        // System.out.printf("[APP Heuristic] UNDER PRESSURE | Processed=%d/%d | Deferred=%d | Backlog=%d | TokensUsed=%.2f\n",
        //         processed, n, buildResult.deferred.size(), deferredQueue.size(), tokensUsed);

        return new ProcessResult(buildResult.executed, tokensUsed, profit, 0, buildResult.deferred);
    }
}

static class AppUpgradeOption {
    int appId;
    int fromWC;       // for writes: WC level; for reads: ReadConcern number
    int toWC;         // for writes: WC level; for reads: ReadConcern number
    int count;
    int originalWc;
    double ratio;
    boolean isReadUpgrade;
    double tokenCostPerTx; // only used for read upgrades

    AppUpgradeOption(int appId, int fromWC, int toWC, int count, double ratio, int originalWc,
                     boolean isReadUpgrade, double tokenCostPerTx) {
        this.appId = appId;
        this.fromWC = fromWC;
        this.toWC = toWC;
        this.count = count;
        this.ratio = ratio;
        this.originalWc = originalWc;
        this.isReadUpgrade = isReadUpgrade;
        this.tokenCostPerTx = tokenCostPerTx;
    }
}

    /**
     * Stores the assigned consistency for a transaction in the batch.
     * For writes: writeConcern is the WC level (0 = deferred).
     * For reads: readConcern is the assigned ReadConcern (potentially upgraded).
     */
    static class ConsistencyAssignment {
        int writeConcern;        // write concern level (for write transactions; 0 = deferred)
        ReadConcern readConcern; // read concern level (for read transactions)
        ReadLevel readLevel;     // read level for read transactions
        boolean isReadOnly;      // true if this is a read transaction

        ConsistencyAssignment(int writeConcern, ReadConcern readConcern, ReadLevel readLevel, boolean isReadOnly) {
            this.writeConcern = writeConcern;
            this.readConcern = readConcern;
            this.readLevel = readLevel;
            this.isReadOnly = isReadOnly;
        }

        boolean isDeferred() {
            // Reads are deferred based on readConcern; writes based on writeConcern
            if (isReadOnly) {
                return readConcern == null;
            }
            return writeConcern == 0;
        }
    }

    /**
     * Get token cost for a single transaction.
     * Reads cost varies by ReadConcern; writes have a flat cost.
     */
    private double getTokenCost(TransactionOption tx) {
        if (tx.isReadOnly) {
            return getReadTokenCost(tx.readConcern, tx.readLevel);
        }
        return writeCost;
    }

    /**
     * Estimate token cost at original (incoming) consistency levels.
     * This is used by the no-upgrade execution path to keep token accounting
     * consistent with BatchProcessor token-cost maps.
     */
    public double estimateTokenCostAtOriginalConsistency(List<TransactionOption> batch) {
        if (batch == null || batch.isEmpty()) {
            return 0.0;
        }
        double total = 0.0;
        for (TransactionOption tx : batch) {
            if (tx == null) {
                continue;
            }
            total += getTokenCost(tx);
        }
        return total;
    }

    public double estimateTokenCostAtOriginalConsistency(TransactionOption tx) {
        if (tx == null) {
            return 0.0;
        }
        return getTokenCost(tx);
    }

    private int tryExecuteDeferredTransaction(
            List<TransactionOption> batch,
            ConsistencyAssignment[] assignments,
            int idx,
            double[] tokensRemainingHolder,
            double[] profitHolder) {
        if (!assignments[idx].isDeferred()) {
            return 0;
        }

        TransactionOption tx = batch.get(idx);
        double cost = getTokenCost(tx);
        if (tokensRemainingHolder[0] < cost) {
            return 0;
        }

        if (tx.isReadOnly) {
            assignments[idx].readConcern = tx.readConcern;
            assignments[idx].readLevel = tx.readLevel;
        } else {
            assignments[idx].writeConcern = tx.minRequiredConsistency;
        }

        tokensRemainingHolder[0] -= cost;
        profitHolder[0] += tx.baseProfit;
        return 1;
    }

    /**
     * Calculate total token cost for all executed (consistencyLevel > 0) transactions.
     */
    private double calculateTotalTokenCost(List<TransactionOption> batch, int[] consistencyLevels) {
        double total = 0;
        for (int i = 0; i < batch.size(); i++) {
            if (consistencyLevels[i] > 0) {
                total += getTokenCost(batch.get(i));
            }
        }
        return total;
    }

    /**
     * Overloaded: Calculate total token cost using ConsistencyAssignment[].
     * Uses the assignment's readConcern (which may have been upgraded) for reads.
     */
    private double calculateTotalTokenCost(List<TransactionOption> batch, ConsistencyAssignment[] assignments) {
        double total = 0;
        for (int i = 0; i < batch.size(); i++) {
            if (!assignments[i].isDeferred()) {
                if (assignments[i].isReadOnly) {
                    total += getReadTokenCost(assignments[i].readConcern, assignments[i].readLevel);
                } else {
                    total += writeCost;
                }
            }
        }
        return total;
    }


    // create method to log the finalBatchAvgTps in csv
    private void logFinalBatchAvgTps(double finalBatchAvgTps) {
        // Append to CSV file
        String logEntry = System.currentTimeMillis() + "," + finalBatchAvgTps + "\n";
        try (java.io.FileWriter fw = new java.io.FileWriter("final_batch_avg_tps_log.csv", true)) {
            fw.write(logEntry);
        } catch (java.io.IOException e) {
            System.err.println("Error logging final batch avg TPS: " + e.getMessage());
        }
    }

    /**
     * Result of building messages - contains both executed and deferred
     */
    public static class BuildResult {
        public final List<ClientMessage> executed;
        public final List<TransactionOption> deferred;
        
        public BuildResult(List<ClientMessage> executed, List<TransactionOption> deferred) {
            this.executed = executed;
            this.deferred = deferred;
        }
    }

    /**
     * Build ClientMessage list from consistency levels, also return deferred transactions
     */
    private BuildResult buildResultMessages(List<TransactionOption> batch, int[] consistencyLevels, Set<String> backLogTransactions) {
        List<ClientMessage> executed = new ArrayList<>();
        List<TransactionOption> deferred = new ArrayList<>();
        
        for (int i = 0; i < batch.size(); i++) {
            int wc = consistencyLevels[i];
            TransactionOption tx = batch.get(i);
            String txId = tx.clientMessage.getT().getId();
            
            if (wc == 0) {
                // Deferred - increment retry count
                tx.retryCount++;
                // Add to backlog set only on FIRST deferral (retryCount just became 1)
                if (tx.retryCount == 1) {
                    // synchronized (backLogTransactions) {
                    //     backLogTransactions.add(txId);
                    // }
                }
                deferred.add(tx);
                continue;
            }
            
            // Transaction is being executed - remove from backlog if it was there
            if(tx.retryCount > 0) {
                // synchronized (backLogTransactions) {
                //     backLogTransactions.remove(txId);
                // }
            }

            // Rebuild Transaction with potentially upgraded readConcern
            Transaction updatedT = tx.clientMessage.getT().toBuilder()
                    .setReadConcern(tx.readConcern)
                    .build();
            ClientMessage msg = ClientMessage.newBuilder()
                    .setT(updatedT)
                    .setWriteConcern(wc)
                    .setCallbackHost(tx.clientHost)
                    .setCallbackPort(tx.clientPort)
                    .build();
            
            // Copy timestamp if present
            if (tx.clientMessage.hasTimeStamp()) {
                msg = msg.toBuilder().setTimeStamp(tx.clientMessage.getTimeStamp()).build();
            }
            
            executed.add(msg);
        }
        return new BuildResult(executed, deferred);
    }

    /**
     * Overloaded: Build ClientMessage list using ConsistencyAssignment[].
     * Uses the assignment's readConcern for read transactions.
     */
    private BuildResult buildResultMessages(List<TransactionOption> batch, ConsistencyAssignment[] assignments, Set<String> backLogTransactions) {
        return buildResultMessages(batch, assignments, backLogTransactions, null);
        }

        private BuildResult buildResultMessages(
            List<TransactionOption> batch,
            ConsistencyAssignment[] assignments,
            Set<String> backLogTransactions,
            Queue<TransactionOption> deferredQueue) {
        List<ClientMessage> executed = new ArrayList<>();
        List<TransactionOption> deferred = new ArrayList<>();

        for (int i = 0; i < batch.size(); i++) {
            ConsistencyAssignment assignment = assignments[i];
            TransactionOption tx = batch.get(i);
            String txId = tx.clientMessage.getT().getId();

            if (assignment.isDeferred()) {
                tx.retryCount++;
                if (tx.retryCount == 1) {
                    // synchronized (backLogTransactions) {
                    //     backLogTransactions.add(txId);
                    // }
                }
                deferred.add(tx);
                if (deferredQueue != null) {
                    if(deferredQueue.size() < 0) { // prevent unbounded growth
                    deferredQueue.add(tx);
                    }
                }
                continue;
            }

            if (tx.retryCount > 0) {
                // synchronized (backLogTransactions) {
                //     backLogTransactions.remove(txId);
                // }
            }

            Transaction updatedT = tx.clientMessage.getT().toBuilder()
                    .setReadConcern(assignment.readConcern)
                    .build();
                if (assignment.readLevel != null) {
                updatedT = updatedT.toBuilder().setReadLevel(assignment.readLevel).build();
                }
            ClientMessage msg = ClientMessage.newBuilder()
                    .setT(updatedT)
                    .setWriteConcern(assignment.writeConcern)
                    .setCallbackHost(tx.clientHost)
                    .setCallbackPort(tx.clientPort)
                    .build();

            if (tx.clientMessage.hasTimeStamp()) {
                msg = msg.toBuilder().setTimeStamp(tx.clientMessage.getTimeStamp()).build();
            }

            executed.add(msg);
        }
        return new BuildResult(executed, deferred);
    }

    /**
     * Count transactions by write concern
     */
    private HashMap<Integer, Integer> countByWriteConcern(int[] consistencyLevels) {
        HashMap<Integer, Integer> counts = new HashMap<>();
        for (int wc : consistencyLevels) {
            if (wc > 0) {
                counts.put(wc, counts.getOrDefault(wc, 0) + 1);
            }
        }
        return counts;
    }

    /**
     * Overloaded: Count transactions by write concern using ConsistencyAssignment[]
     */
    private HashMap<Integer, Integer> countByWriteConcern(ConsistencyAssignment[] assignments) {
        HashMap<Integer, Integer> counts = new HashMap<>();
        for (ConsistencyAssignment a : assignments) {
            if (!a.isDeferred() && !a.isReadOnly) {
                counts.put(a.writeConcern, counts.getOrDefault(a.writeConcern, 0) + 1);
            }
        }
        return counts;
    }

    /**
     * First executes all transactions at minimum consistency, then upgrades to maximize profit
     * This is a hybrid approach that ensures throughput first, then optimizes for profit
     * <p>
     * Algorithm:
     * Phase 1: Execute ALL transactions at their minimum required consistency level
     * Phase 2: Only if ALL transactions were processed, use remaining budget to upgrade
     * transactions up to majority level to maximize profit
     */
    public ProcessResult processForThroughputThenProfit(
            List<TransactionOption> transactions,
            double budget,
            double currentTokens,
            boolean allowUpgrades,
            HashMap<Integer, Double> writeConcernCosts) {

        double usedBudget = 0.0;
        double profit = 0.0;
        List<ClientMessage> result = new ArrayList<>();
        int majority = (NUM_OF_SERVERS / 2) + 1;
        int n = transactions.size();

        int[] consistencyLevels = new int[n];

        // Sort by minimum consistency first, then by profit
        transactions.sort((a, b) -> {
            if (a.minRequiredConsistency == b.minRequiredConsistency) {
                return Double.compare(b.baseProfit, a.baseProfit);
            }
            return a.minRequiredConsistency - b.minRequiredConsistency;
        });

        // Phase 1: Execute all transactions at minimum consistency + free upgrades
        boolean allTransactionsProcessed = true;
        for (int i = 0; i < n; i++) {
            TransactionOption tx = transactions.get(i);
            int currentWC = tx.minRequiredConsistency;
            double txProfit = tx.baseProfit;

            // Apply free upgrades immediately and accumulate profit
            while (currentWC < majority) {
                double currentCost = writeConcernCosts.get(currentWC);
                double nextCost = writeConcernCosts.get(currentWC + 1);

                // Use relative epsilon for comparison: |a - b| <= eps * max(|a|, |b|)
                double maxCost = Math.max(Math.abs(currentCost), Math.abs(nextCost));
                double relativeEps = EPS * Math.max(1.0, maxCost);

                if (nextCost <= currentCost + relativeEps) {
                    currentWC++;
                    // Add profit for each upgrade step
                    txProfit += (currentWC == majority)
                            ? tx.extraMajorityProfit
                            : tx.extraIntermediateProfit;
                } else {
                    break;
                }
            }

            double cost = writeConcernCosts.get(currentWC);

            // Check if we can afford this transaction (with tolerance)
            double availableBudget = budget - usedBudget;
            double relativeEps = EPS * Math.max(1.0, Math.max(Math.abs(availableBudget), Math.abs(cost)));

            if (cost <= availableBudget + relativeEps) {
                usedBudget += cost;
                profit += txProfit;
                consistencyLevels[i] = currentWC;
            } else {
                allTransactionsProcessed = false;
                break;
            }
        }

        double remainingBudget = budget - usedBudget;
        int transactionsUpgraded = 0;

        // Phase 2: Upgrade transactions using remaining budget (greedy)
        if (allTransactionsProcessed && remainingBudget > EPS && allowUpgrades) {
            PriorityQueue<UpgradeOption> pq = new PriorityQueue<>(
                    (a, b) -> Double.compare(b.ratio, a.ratio)
            );

            // Initialize possible upgrades from actual current level
            for (int i = 0; i < n; i++) {
                TransactionOption tx = transactions.get(i);
                int currentWC = consistencyLevels[i];

                if (currentWC < majority) {
                    double currentCost = writeConcernCosts.get(currentWC);
                    double nextCost = writeConcernCosts.get(currentWC + 1);
                    double upgradeCost = nextCost - currentCost;

                    // Skip if upgrade cost is negative or negligible
                    if (upgradeCost > EPS) {
                        double nextProfit = (currentWC + 1 == majority)
                                ? tx.extraMajorityProfit
                                : tx.extraIntermediateProfit;
                        double ratio = nextProfit / upgradeCost;
                        pq.add(new UpgradeOption(i, currentWC, currentWC + 1, upgradeCost, nextProfit, ratio));
                    }
                }
            }

            // Greedy upgrading loop
            while (!pq.isEmpty() && remainingBudget > EPS) {
                UpgradeOption opt = pq.poll();

                // Check if upgrade is still valid and affordable
                if (opt.toWC > consistencyLevels[opt.txIndex]) {
                    double relativeEps = EPS * Math.max(1.0, Math.max(Math.abs(remainingBudget), Math.abs(opt.upgradeCost)));

                    if (opt.upgradeCost <= remainingBudget + relativeEps) {
                        remainingBudget -= opt.upgradeCost;
                        usedBudget += opt.upgradeCost;
                        profit += opt.additionalProfit;
                        consistencyLevels[opt.txIndex] = opt.toWC;
                        transactionsUpgraded++;

                        // Generate the next upgrade for this transaction (if possible)
                        if (opt.toWC < majority) {
                            int nextWC = opt.toWC + 1;
                            double currentCost = writeConcernCosts.get(opt.toWC);
                            double nextCost = writeConcernCosts.get(nextWC);
                            double nextUpgradeCost = nextCost - currentCost;

                            // Only add if cost increase is meaningful
                            if (nextUpgradeCost > EPS) {
                                double nextProfit = (nextWC == majority)
                                        ? transactions.get(opt.txIndex).extraMajorityProfit
                                        : transactions.get(opt.txIndex).extraIntermediateProfit;
                                double nextRatio = nextProfit / (nextUpgradeCost + EPS);
                                pq.add(new UpgradeOption(opt.txIndex, opt.toWC, nextWC, nextUpgradeCost, nextProfit, nextRatio));
                            }
                        }
                    }
                }
            }
        }

        // Generate final result messages after upgrades
        for (int i = 0; i < n; i++) {
            TransactionOption tx = transactions.get(i);
            int wc = consistencyLevels[i];
            // skip the unprocessed transactions
            if (wc == 0) continue;
            ClientMessage msg = ClientMessage.newBuilder()
                    .setT(tx.clientMessage.getT())
                    .setWriteConcern(wc)
                    .build();
            result.add(msg);
        }

        return new ProcessResult(result, usedBudget, profit, transactionsUpgraded);
    }

    public ProcessResult processTransactionUnderutilised(List<TransactionOption> transactions, double budget, boolean allowUpgrades, HashMap<Integer, Double> writeConcernCosts) {

        int noOfTransactions = transactions.size();
        int tokensUsed = noOfTransactions;
        int majority = (NUM_OF_SERVERS / 2) + 1;
        int transactionsUpgraded = 0;

        List<ClientMessage> result = new ArrayList<>();

        int[] consistencyLevels = new int[noOfTransactions];

        double profit = 0;
        for(int i = 0; i < noOfTransactions; i++) {
            TransactionOption tx = transactions.get(i);
            consistencyLevels[i] = tx.minRequiredConsistency;
            profit += tx.baseProfit;
        }


        if(allowUpgrades) {
            PriorityQueue<UpgradeOption> pq = new PriorityQueue<>(
                    (a, b) -> Double.compare(b.ratio, a.ratio)
            );

            // Initialize possible upgrades from actual current level
            for (int i = 0; i < noOfTransactions; i++) {
                TransactionOption tx = transactions.get(i);
                int currentWC = consistencyLevels[i];

                if (currentWC < majority) {
                    double currentCost = writeConcernCosts.get(currentWC);
                    double nextCost = writeConcernCosts.get(currentWC + 1);
                    double upgradeCost = nextCost - currentCost;

                    // Skip if upgrade cost is negative or negligible
                        double nextProfit = (currentWC + 1 == majority)
                                ? tx.extraMajorityProfit
                                : tx.extraIntermediateProfit;
                        double ratio = nextProfit / (upgradeCost + EPS);
                        pq.add(new UpgradeOption(i, currentWC, currentWC + 1, upgradeCost, nextProfit, ratio));
                }
            }

            boolean[] upgraded = new boolean[noOfTransactions];

            // Greedy upgrading loop
            while (!pq.isEmpty() &&  budget > EPS) {

                UpgradeOption opt = pq.poll();

                // Check if upgrade is still valid and affordable
                if (opt.toWC > consistencyLevels[opt.txIndex] && opt.fromWC == consistencyLevels[opt.txIndex]){

                    if (opt.upgradeCost <= budget + EPS) {
                        budget -= opt.upgradeCost;
                        profit += opt.additionalProfit;
                        consistencyLevels[opt.txIndex] = opt.toWC;
                        if(!upgraded[opt.txIndex]) {
                            transactionsUpgraded++;
                        }
                        upgraded[opt.txIndex] = true;


                        // Generate the next upgrade for this transaction (if possible)
                        if (opt.toWC < majority) {
                            int nextWC = opt.toWC + 1;
                            double currentCost = writeConcernCosts.get(opt.toWC);
                            double nextCost = writeConcernCosts.get(nextWC);
                            double nextUpgradeCost = nextCost - currentCost;

                            // Only add if cost increase is meaningful
                                double nextProfit = (nextWC == majority)
                                        ? transactions.get(opt.txIndex).extraMajorityProfit
                                        : transactions.get(opt.txIndex).extraIntermediateProfit;
                                double nextRatio = nextProfit / (nextUpgradeCost + EPS);
                                pq.add(new UpgradeOption(opt.txIndex, opt.toWC, nextWC, nextUpgradeCost, nextProfit, nextRatio));}
                    }
                }
            }
            System.out.println("budget left: " + budget);
        }

        HashMap<Integer,Integer> map = new HashMap<>();


        // Generate final result messages after upgrades
        for (int i = 0; i < noOfTransactions; i++) {
            TransactionOption tx = transactions.get(i);
            int wc = consistencyLevels[i];
            if(tx.minRequiredConsistency != wc) {
                map.put(wc, map.getOrDefault(wc,0)+1);
            }
            // skip the unprocessed transactions
            if (wc == 0) continue;
            ClientMessage msg = ClientMessage.newBuilder()
                    .setT(tx.clientMessage.getT())
                    .setWriteConcern(wc)
                    .setCallbackHost(tx.clientHost)
                    .setCallbackPort(tx.clientPort)
                    .build();
            result.add(msg);
        }
        System.out.println("Upgrade map: " + map.toString());
        return new ProcessResult(result, tokensUsed, profit, transactionsUpgraded);
    }

    public ProcessResult processTransactions(List<TransactionOption> batch, double budget, double currentTokens, boolean allowUpgrades, HashMap<Integer, Double> writeConcernCosts) {
        // Calculate total cost of processing all transactions at minimum consistency
        double totalMinCost = 0;
        // for testing

        // writeConcernCosts.put(1,1.0);
        // writeConcernCosts.put(2,1.0);
        
        for (TransactionOption tx : batch) {
            totalMinCost += writeConcernCosts.get(tx.minRequiredConsistency);
        }
        
        // If total cost exceeds budget, process subset of transactions
        if (totalMinCost > budget + EPS) {
            double profit = 0;
            double usedBudget = 0;
            // Sort by minimum consistency to process cheaper transactions first
            Collections.sort(batch, Comparator.comparingInt(a -> a.minRequiredConsistency));
            
            List<ClientMessage> result = new ArrayList<>();
            List<TransactionOption> toProcess = new ArrayList<>();
            
            // Process transactions until budget is exhausted
            for (TransactionOption tx : batch) {
                double txCost = writeConcernCosts.get(tx.minRequiredConsistency);
                if (usedBudget + txCost <= budget + EPS) {
                    usedBudget += txCost;
                    profit += tx.baseProfit;
                    toProcess.add(tx);
                    ClientMessage msg = ClientMessage.newBuilder()
                            .setT(tx.clientMessage.getT())
                            .setWriteConcern(tx.minRequiredConsistency)
                            .setCallbackHost(tx.clientHost)
                            .setCallbackPort(tx.clientPort)
                            .build();
                    result.add(msg);
                } else {
                    break; // Budget exhausted
                }
            }
            
            System.out.println("Budget exceeded: processed " + toProcess.size() + "/" + batch.size() + " transactions, used budget: " + usedBudget);
            return new ProcessResult(result, toProcess.size(), profit, 0);
        } else {
            // Budget sufficient for all transactions at min consistency + upgrades
            return processTransactionUnderutilised(batch, budget, false, writeConcernCosts);
        }
    }
    /**
     * Helper class to represent an upgrade opportunity
     */
    class UpgradeOption {
        int txIndex;            // index of transaction in list
        int fromWC;             // starting write concern
        int toWC;               // upgraded write concern
        double upgradeCost;     // extra token cost
        double additionalProfit;
        double ratio;

        public UpgradeOption(int txIndex, int fromWC, int toWC,
                             double upgradeCost, double additionalProfit, double ratio) {
            this.txIndex = txIndex;
            this.fromWC = fromWC;
            this.toWC = toWC;
            this.upgradeCost = upgradeCost;
            this.additionalProfit = additionalProfit;
            this.ratio = ratio;
        }
    }
}