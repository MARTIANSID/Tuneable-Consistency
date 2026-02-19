 
package org.example.Utility;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Map;

import org.ds.paxos.ClientMessage;

public class BatchProcessor {

    private final int NUM_OF_SERVERS;
    final double EPS = 1e-9;

    // ========== Warmup Phase Constants ==========
    private static final long WARMUP_DURATION_MS = 7000;  // 7 seconds warmup
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

    private static final HashMap<Integer, Double> MIN_LATENCY_MAP = new HashMap<>() {{
        put(1, 80.0);  // 50 ms for W:1
        put(2, 3000.0); // 150 ms for W:2
     }};

     private static final double MAX_LATENCY = 80.0; // Max average latency in ms
     private static final double UPGRADE_LATENCY_THRESHOLD = 0.85; // Need 15% headroom to start upgrading
     private static final double UPGRADE_LATENCY_FLOOR = 0.95; // Stop upgrading at 10% above max latency
     private static final double MAX_LOAD = 12500;

    /**
     * Check if system is still in warmup phase (first 7 seconds)
     */
    private boolean isInWarmupPhase() {
        return (System.currentTimeMillis() - systemStartTime) < WARMUP_DURATION_MS;
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
            return MIN_LATENCY_MAP.getOrDefault(writeConcern, 0.0);
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
            if (avgTPS >= MIN_TPS * UPGRADE_THRESHOLD && backLogTransactions.isEmpty() && currentTPS > MIN_TPS) {
                
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
        Set<String> backLogTransactions) {

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
            if (avgLatency <= MAX_LATENCY * UPGRADE_LATENCY_THRESHOLD && backLogTransactions.isEmpty() && currentLatency < MAX_LATENCY && incomingRateOfTransactions < MAX_LOAD) {
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
    Set<String> backLogTransactions) {

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

    // Step 2: Single pass — build count map, profit lookup maps, and batch index
    // appWcCount:              appId -> (wc -> count)
    // appIntermediateProfitMap: appId -> extraIntermediateProfit  (uniform per appId)
    // appMajorityProfitMap:     appId -> extraMajorityProfit      (uniform per appId)
    // appWcBatchIndex:          appId -> (wc -> list of batch indices)
    HashMap<Integer, HashMap<Integer, Integer>> appWcCount = new HashMap<>();
    HashMap<Integer, Double> appIntermediateProfitMap = new HashMap<>();
    HashMap<Integer, Double> appMajorityProfitMap = new HashMap<>();
    HashMap<Integer, HashMap<Integer, List<Integer>>> appWcBatchIndex = new HashMap<>();
    HashMap<Integer, Double> appBaseProfitMap = new HashMap<>();


    for (int i = 0; i < n; i++) {
        TransactionOption tx = batch.get(i);
        int appId = tx.applicationId;
        int wc = tx.minRequiredConsistency;

        appWcCount
            .computeIfAbsent(appId, k -> new HashMap<>())
            .merge(wc, 1, Integer::sum);

        appIntermediateProfitMap.putIfAbsent(appId, tx.extraIntermediateProfit);
        appMajorityProfitMap.putIfAbsent(appId, tx.extraMajorityProfit);

        appWcBatchIndex
            .computeIfAbsent(appId, k -> new HashMap<>())
            .computeIfAbsent(wc, k -> new ArrayList<>())
            .add(i);
        appBaseProfitMap.putIfAbsent(appId, tx.baseProfit);
    }

    System.out.println("App-WC Count Map:");
    System.out.println(appWcCount);

    // Step 3: Calculate avgLatency with all transactions at min consistency
    double avgLatency = calculateAvgLatency(currentLatency, consistencyLevels, wcLatencyMap);

    System.out.printf("[APP Heuristic] currentLatency=%.2f | avgLatency=%.2f | maxLatency=%.2f\n",
            currentLatency, avgLatency, MAX_LATENCY);

    System.out.print("[WriteConcern Latency] ");
    for (int wc = 1; wc <= majority; wc++) {
        System.out.printf("W:%d=%.2f ms | ", wc, wcLatencyMap.get(wc));
    }
    System.out.println();

    // Step 4: Check if we can execute all
    if (avgLatency <= MAX_LATENCY) {

        for (int i = 0; i < n; i++) {
            profit += batch.get(i).baseProfit;
        }

        // Step 5: Upgrade phase
        if (avgLatency <= MAX_LATENCY * UPGRADE_LATENCY_THRESHOLD
                && backLogTransactions.isEmpty()
                && currentLatency < MAX_LATENCY
                && incomingRateOfTransactions < MAX_LOAD) {

            // PQ: ranked by per-transaction ratio (profit per unit latency cost)
            PriorityQueue<AppUpgradeOption> pq = new PriorityQueue<>(
                    (a, b) -> Double.compare(b.ratio, a.ratio)
            );

            // Seed PQ: for every app, for every WC level that can go higher
            for (Map.Entry<Integer, HashMap<Integer, Integer>> appEntry : appWcCount.entrySet()) {
                int appId = appEntry.getKey();
                for (Map.Entry<Integer, Integer> wcEntry : appEntry.getValue().entrySet()) {
                    int fromWC = wcEntry.getKey();
                    int count = wcEntry.getValue();
                    if (fromWC >= majority || count == 0) continue;

                    int toWC = fromWC + 1;
                    double latencyIncPerTx = getMaxLatency(toWC, wcLatencyMap) - getMaxLatency(fromWC, wcLatencyMap);

                    // O(1) profit lookup — keyed by appId only
                    double perTxProfit = (toWC == majority)
                            ? appMajorityProfitMap.get(appId)
                            : appIntermediateProfitMap.get(appId);

                    double ratio = (latencyIncPerTx > EPS) ? perTxProfit / latencyIncPerTx : Double.MAX_VALUE;
                    pq.add(new AppUpgradeOption(appId, fromWC, toWC, count, ratio));
                }
            }

            // Step 6: Greedy upgrade loop — only counts are mutated, no array writes
            while (!pq.isEmpty()) {
                AppUpgradeOption opt = pq.poll();

                int currentCount = appWcCount.getOrDefault(opt.appId, new HashMap<>()).getOrDefault(opt.fromWC, 0);
                if (currentCount == 0) continue;

                double latencyIncPerTx = getMaxLatency(opt.toWC, wcLatencyMap) - getMaxLatency(opt.fromWC, wcLatencyMap);
                double remainingHeadroom = (MAX_LATENCY * UPGRADE_LATENCY_FLOOR - avgLatency) * (n + 1);

                int maxAffordable = (latencyIncPerTx > EPS)
                        ? (int) Math.floor(remainingHeadroom / latencyIncPerTx)
                        : currentCount;

                int toUpgrade = Math.min(currentCount, maxAffordable);
                if (toUpgrade <= 0) continue;

                // Only update counts — no array writes yet
                avgLatency += (latencyIncPerTx * toUpgrade) / (n + 1);
                appWcCount.get(opt.appId).put(opt.fromWC, currentCount - toUpgrade);
                appWcCount.get(opt.appId).merge(opt.toWC, toUpgrade, Integer::sum);
                transactionsUpgraded += toUpgrade;

                // O(1) profit lookup — keyed by appId only
                profit += toUpgrade * ((opt.toWC == majority)
                        ? appMajorityProfitMap.get(opt.appId)
                        : appIntermediateProfitMap.get(opt.appId));

                // Seed next level if still below majority
                if (opt.toWC < majority) {
                    int nowAtToWC = appWcCount.get(opt.appId).getOrDefault(opt.toWC, 0);
                    if (nowAtToWC > 0) {
                        int nextWC = opt.toWC + 1;
                        double nextLatencyIncPerTx = getMaxLatency(nextWC, wcLatencyMap) - getMaxLatency(opt.toWC, wcLatencyMap);

                        // O(1) profit lookup — keyed by appId only
                        double nextPerTxProfit = (nextWC == majority)
                                ? appMajorityProfitMap.get(opt.appId)
                                : appIntermediateProfitMap.get(opt.appId);

                        double nextRatio = (nextLatencyIncPerTx > EPS) ? nextPerTxProfit / nextLatencyIncPerTx : Double.MAX_VALUE;
                        pq.add(new AppUpgradeOption(opt.appId, opt.toWC, nextWC, nowAtToWC, nextRatio));
                    }
                }
            }

            // Step 7: Single final pass — assign consistencyLevels based on final appWcCount
            // Copy counts so we can consume quotas
            HashMap<Integer, HashMap<Integer, Integer>> appWcRemaining = new HashMap<>();
            for (Map.Entry<Integer, HashMap<Integer, Integer>> appEntry : appWcCount.entrySet()) {
                appWcRemaining.put(appEntry.getKey(), new HashMap<>(appEntry.getValue()));
            }

            System.out.println("Final App-WC Count After Upgrades:");
            System.out.println(appWcRemaining);

            for (int i = 0; i < n; i++) {
                TransactionOption tx = batch.get(i);
                int appId = tx.applicationId;
                int minWC = tx.minRequiredConsistency;
                HashMap<Integer, Integer> wcRemaining = appWcRemaining.get(appId);

                // Assign highest available WC >= minWC that still has quota
                int assignedWC = minWC;
                for (int wc = majority; wc >= minWC; wc--) {
                    int remaining = wcRemaining.getOrDefault(wc, 0);
                    if (remaining > 0) {
                        assignedWC = wc;
                        wcRemaining.put(wc, remaining - 1);
                        break;
                    }
                }

                consistencyLevels[i] = assignedWC;
            }
        }

        BuildResult buildResult = buildResultMessages(batch, consistencyLevels, backLogTransactions);
        HashMap<Integer, Integer> wcMix = countByWriteConcern(consistencyLevels);
        System.out.printf("[APP Heuristic] EXECUTED ALL | Mix=%s | Upgraded=%d | Backlog=%d | FinalAvgLatency=%.2f\n",
                wcMix, transactionsUpgraded, backLogTransactions.size(), avgLatency);

        return new ProcessResult(buildResult.executed, n, profit, transactionsUpgraded, buildResult.deferred);
    }

    // Step 8: avgLatency > maxLatency — pressure path, app-id priority
    else {
        for (int i = 0; i < n; i++) consistencyLevels[i] = 0;

        int minToProcess = Math.max(1, (int) (n * 0.7));
        int processed = 0;
        double latencySum = currentLatency;

        // Iterate app IDs descending (highest appId = highest profit first)
        // Sort app IDs by base profit descending
        List<Integer> sortedAppIds = new ArrayList<>(appWcCount.keySet());
        sortedAppIds.sort((a, b) -> Double.compare(appBaseProfitMap.get(b), appBaseProfitMap.get(a)));

        outer:
        for (int appId : sortedAppIds) {

            for (int wc = 1; wc <= majority; wc++) {
                if (!appWcBatchIndex.get(appId).containsKey(wc)) continue;

                // Use batch index directly — no scanning
                for (int idx : appWcBatchIndex.get(appId).get(wc)) {
                    TransactionOption tx = batch.get(idx);
                    boolean mustExecute = tx.retryCount >= 2;

                    consistencyLevels[idx] = tx.minRequiredConsistency;
                    int minLatency = getMaxLatency(tx.minRequiredConsistency, wcLatencyMap);
                    latencySum += minLatency;

                    // processed + 1 for current tx being considered + 1 for currentLatency slot
                    double newAvgLatency = latencySum / (processed + 2);

                    if (mustExecute || processed < minToProcess || newAvgLatency <= MAX_LATENCY
                            || wcTpsMap.get((NUM_OF_SERVERS) / 2 + 1) > MIN_TPS_OF_MAJORITY) {
                        profit += tx.baseProfit;
                        processed++;
                    } else {
                        latencySum -= minLatency;  // undo latency addition
                        consistencyLevels[idx] = 0;
                        break outer;
                    }
                }
            }
        }

        finalBatchAvgLatency = latencySum / (processed + 1);
        BuildResult buildResult = buildResultMessages(batch, consistencyLevels, backLogTransactions);

        System.out.printf("[APP Heuristic] UNDER PRESSURE | Processed=%d/%d | Deferred=%d | Backlog=%d | FinalAvgLatency=%.2f\n",
                processed, n, buildResult.deferred.size(), backLogTransactions.size(), finalBatchAvgLatency);

        return new ProcessResult(buildResult.executed, processed, profit, 0, buildResult.deferred);
    }
}

static class AppUpgradeOption {
    int appId;
    int fromWC;
    int toWC;
    int count;
    double ratio; // per single transaction — not total

    AppUpgradeOption(int appId, int fromWC, int toWC, int count, double ratio) {
        this.appId = appId;
        this.fromWC = fromWC;
        this.toWC = toWC;
        this.count = count;
        this.ratio = ratio;
    }
}

static class AppUpgradeOption {
    int appId;
    int fromWC;
    int toWC;
    int count;
    double ratio; // per single transaction — not total

    AppUpgradeOption(int appId, int fromWC, int toWC, int count, double ratio) {
        this.appId = appId;
        this.fromWC = fromWC;
        this.toWC = toWC;
        this.count = count;
        this.ratio = ratio;
    }
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
                    backLogTransactions.add(txId);
                }
                deferred.add(tx);
                continue;
            }
            
            // Transaction is being executed - remove from backlog if it was there
            if(tx.retryCount > 0) {
                backLogTransactions.remove(txId);
            }


            ClientMessage msg = ClientMessage.newBuilder()
                    .setT(tx.clientMessage.getT())
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