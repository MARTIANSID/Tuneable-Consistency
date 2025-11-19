package org.example.Utility;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.ds.paxos.ClientMessage;

public class BatchProcessor {

    private final int NUM_OF_SERVERS;
    final double EPS = 1e-9;

    public BatchProcessor(int numOfServers) {
        this.NUM_OF_SERVERS = numOfServers;
    }

    /**
     * First executes all transactions at minimum consistency, then upgrades to maximize profit
     * This is a hybrid approach that ensures throughput first, then optimizes for profit
     * <p>
     * Algorithm:
     * Phase 1: Execute ALL transactions at their minimum required consistency level
     * Phase 2: Only if ALL transactions were processed, use remaining tokens to upgrade
     * transactions up to majority level to maximize profit
     */
    public ProcessResult processForThroughputThenProfit(
            List<TransactionOption> transactions,
            double currentTokens,
            boolean allowUpgrades,
            HashMap<Integer, Double> writeConcernCosts) {

        double usedTokens = 0.0;
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
            double availableTokens = currentTokens - usedTokens;
            double relativeEps = EPS * Math.max(1.0, Math.max(Math.abs(availableTokens), Math.abs(cost)));

            if (cost <= availableTokens + relativeEps) {
                usedTokens += cost;
                profit += txProfit;
                consistencyLevels[i] = currentWC;
            } else {
                allTransactionsProcessed = false;
                break;
            }
        }

        double remainingTokens = currentTokens - usedTokens;
        int transactionsUpgraded = 0;

        // Phase 2: Upgrade transactions using remaining tokens (greedy)
        if (allTransactionsProcessed && remainingTokens > EPS && allowUpgrades) {
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
            while (!pq.isEmpty() && remainingTokens > EPS) {
                UpgradeOption opt = pq.poll();

                // Check if upgrade is still valid and affordable
                if (opt.toWC > consistencyLevels[opt.txIndex]) {
                    double relativeEps = EPS * Math.max(1.0, Math.max(Math.abs(remainingTokens), Math.abs(opt.upgradeCost)));

                    if (opt.upgradeCost <= remainingTokens + relativeEps) {
                        remainingTokens -= opt.upgradeCost;
                        usedTokens += opt.upgradeCost;
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
                                double nextRatio = nextProfit / nextUpgradeCost;

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

        return new ProcessResult(result, usedTokens, profit, transactionsUpgraded);
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

    public ProcessResult processTransactions(List<TransactionOption> batch, double currentTokens, boolean allowUpgrades, HashMap<Integer, Double> writeConcernCosts) {
        List<TransactionOption> toProcess = batch;
        double budget = 10000;
        if(batch.size() > currentTokens) {
            double profit = 0;
            // backlog
            Collections.sort(batch, Comparator.comparingInt(a -> a.minRequiredConsistency));
            toProcess = batch.subList(0, (int)currentTokens);
            List<ClientMessage> result = new ArrayList<>();
            for(int i = 0; i < toProcess.size(); i++) {
                TransactionOption tx = toProcess.get(i);
                profit += tx.baseProfit;
                ClientMessage msg = ClientMessage.newBuilder()
                        .setT(tx.clientMessage.getT())
                        .setWriteConcern(tx.minRequiredConsistency)
                        .setCallbackPort(tx.clientPort)
                        .setCallbackHost(tx.clientHost)
                        .build();
                result.add(msg);
            }
            return new ProcessResult(result, toProcess.size(), profit, 0);
        } else {
            return processTransactionUnderutilised(batch, budget, allowUpgrades, writeConcernCosts);
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
