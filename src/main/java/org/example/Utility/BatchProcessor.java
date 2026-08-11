 
package org.example.Utility;

import java.util.ArrayList;
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
    // Avg-latency caps for the execute/upgrade decision (from config; see
    // ExperimentConfig.batchProcessor). Applied per batch depending on role.
    private static volatile double LEADER_MAX_LATENCY_MS = 60.0;
    private static volatile double FOLLOWER_MAX_LATENCY_MS = 50.0;

    /** Apply tuning configuration. Must run before batch processing starts. */
    public static void applyConfig(ExperimentConfig config) {
        LEADER_MAX_LATENCY_MS = config.batchProcessor.leaderMaxLatencyMs;
        FOLLOWER_MAX_LATENCY_MS = config.batchProcessor.followerMaxLatencyMs;
    }


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
    
    private static final double writeCost = 15;
    // write cost = 22 (for good results)

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
        put(RC_KEY_LINEARIZABLE_ALL, 11.0);

        // linearizable cost : 15
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
        return new ProcessResult(new ArrayList<>(), 0, 0, 0, 0);
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
    MAX_LATENCY = isLeader ? LEADER_MAX_LATENCY_MS : FOLLOWER_MAX_LATENCY_MS;
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

        return new ProcessResult(buildResult.executed, tokensUsedForResult, profit, transactionsUpgraded, buildResult.deferred, finalTokenCost);
    }

    // Step 8: avgLatency > maxLatency — pressure path, token-budget based
    else {
        if (!pressureModeEnabled) {
            for (int i = 0; i < n; i++) {
                profit += batch.get(i).baseProfit;
            }

            double finalTokenCost = calculateTotalTokenCost(batch, assignments);
            double tokensUsed = 0.0;
            BuildResult buildResult = buildResultMessages(batch, assignments, backLogTransactions, null);
            return new ProcessResult(buildResult.executed, tokensUsed, profit, 0, List.of(), finalTokenCost);
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

        return new ProcessResult(buildResult.executed, tokensUsed, profit, 0, buildResult.deferred, tokensUsed);
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
                    if(deferredQueue.size() < 100) { // prevent unbounded growth
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

            // Reflect the executed (possibly upgraded) consistency in the embedded
            // transaction too, so downstream ACKs report the truth to the client.
            Transaction updatedT = tx.clientMessage.getT().toBuilder()
                    .setReadConcern(assignment.readConcern)
                    .setWriteConcern(assignment.writeConcern)
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

}