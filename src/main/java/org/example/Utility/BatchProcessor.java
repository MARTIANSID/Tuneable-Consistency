package org.example.Utility;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.ds.paxos.ClientMessage;

public class BatchProcessor {

    private final int NUM_OF_SERVERS;
    private final double scale;
    private final ConcurrentHashMap<Integer, Double> writeConcernCosts;

    public BatchProcessor(int numOfServers, double scale, ConcurrentHashMap<Integer, Double> writeConcernCosts) {
        this.NUM_OF_SERVERS = numOfServers;
        this.scale = scale;
        this.writeConcernCosts = writeConcernCosts;
    }

    /**
     * Use this when throughput is lower than expected
     * Prioritizes lower consistency levels to maximize transaction count
     */
    public ProcessResult processForThroughput(List<TransactionOption> transactions, double currentTokens, int minTransactions) {
        // n*log(n)
        Collections.sort(transactions, (a, b) -> {
            // if consistency is same select transaction with higher profit
            if (a.minRequiredConsistency == b.minRequiredConsistency) return Double.compare(b.baseProfit, a.baseProfit);

            // select the lower consistency transaction
            return (a.minRequiredConsistency - b.minRequiredConsistency);
        });

        List<ClientMessage> selected = new ArrayList<>();

        double usedTokens = 0.0, profit = 0;

        int index = 0;

        for (TransactionOption t : transactions) {
            int minConsistencyOfTransaction = t.minRequiredConsistency;
            double profitForMinConsistency = t.baseProfit;
            double tokenCostOfTransaction = tokenCost(minConsistencyOfTransaction);

            if (Double.compare(tokenCostOfTransaction + usedTokens, currentTokens * scale) <= 0) {
                usedTokens += tokenCostOfTransaction;
                ClientMessage.Builder cmBuilder = t.clientMessage.toBuilder();
                // I update the writeConcern of the transaction
                cmBuilder.setWriteConcern(minConsistencyOfTransaction);

                profit += profitForMinConsistency;
                selected.add(cmBuilder.build());
                // added this because we do not want the current batch to consume all the tokens, we want to get the required throughput
                // we might want to process more transactions if lets say more w:1 are left?? because w:1 is pretty cheap we can use some tokens for it
                if (selected.size() >= minTransactions && (index + 1) < transactions.size() && transactions.get(index + 1).minRequiredConsistency != 1)
                    return new ProcessResult(selected, (usedTokens / scale), profit, 0);
            } else {
                // we break here because now the token cost will increase because consistency levels are only going to increase
                break;
            }
            index++;
        }
        // here no transactions are upgraded so I simply pass 0
        return new ProcessResult(selected, usedTokens / scale, profit, 0);
    }

    /**
     * Use this when throughput is higher or equal to expected value
     * Optimizes for profit using dynamic programming
     */
    public ProcessResult processForProfit(List<TransactionOption> transactions, double currentTokens, int minTransactions) {
        int n = transactions.size();
        int maxTokens = (int) Math.ceil(currentTokens * scale);
        int majorityLevel = (NUM_OF_SERVERS / 2) + 1;

        class State {
            double profit;
            int count;
            int prevT;
            int consistency;
            boolean taken;

            State(double profit, int count, int prevT, int consistency, boolean taken) {
                this.profit = profit;
                this.count = count;
                this.prevT = prevT;
                this.consistency = consistency;
                this.taken = taken;
            }
        }

        // **** we can reduce the complexity of this DP by tuning the batch timing ****
        // complexity of this is (no of transaction in the batch) * (max tokens allowed)

        State[][] dp = new State[n + 1][maxTokens + 1];
        dp[0][0] = new State(0, 0, -1, 0, false);

        int maxExecutedTransactions = 0;

        for (int i = 0; i < n; i++) {
            TransactionOption tx = transactions.get(i);

            for (int t = 0; t <= maxTokens; t++) {
                if (dp[i][t] == null) continue;

                // this the case where we skip the transaction
                if (dp[i + 1][t] == null || dp[i + 1][t].profit < dp[i][t].profit) {
                    dp[i + 1][t] = new State(dp[i][t].profit, dp[i][t].count, t, 0, false);
                }

                // we try all possible writeConcern
                for (int wc : writeConcernCosts.keySet()) {
                    int cost = (int) tokenCost(wc);

                    if (wc < tx.minRequiredConsistency) continue;

                    double profit = tx.baseProfit;

                    if (wc >= majorityLevel) {
                        profit += tx.extraMajorityProfit;
                    } else if (wc > tx.minRequiredConsistency) {
                        profit += tx.extraIntermediateProfit * (wc - tx.minRequiredConsistency);
                    }

                    // obviously it should not exceed the maxTokens limit
                    if (t + cost <= maxTokens) {
                        int nt = t + cost;
                        double newProfit = dp[i][t].profit + profit;
                        int newCount = dp[i][t].count + 1;

                        if (dp[i + 1][nt] == null || dp[i + 1][nt].profit < newProfit) {
                            dp[i + 1][nt] = new State(newProfit, newCount, t, wc, true);
                            // we calculate the max number of transactions we can execute
                            maxExecutedTransactions = Math.max(maxExecutedTransactions, newCount);
                        }
                    }
                }
            }
        }

        double bestProfit = -1;
        int bestT = -1, transactionsUpgraded = 0;

        for (int t = 0; t <= maxTokens; t++) {
            // we use Math.min because the minTransactions could be higher than the number of transactions that we can execute
            if (dp[n][t] != null && dp[n][t].count >= Math.min(maxExecutedTransactions, minTransactions)) {
                if (dp[n][t].profit > bestProfit) {
                    bestProfit = dp[n][t].profit;
                    bestT = t;
                }
            }
        }

        if (bestT == -1) return new ProcessResult(new ArrayList<>(), 0.0, bestProfit, 0);

        List<ClientMessage> result = new ArrayList<>();
        int t = bestT;

        for (int i = n; i >= 1; i--) {
            State s = dp[i][t];
            if (s == null) break;

            if (s.taken) {
                TransactionOption tx = transactions.get(i - 1);
                ClientMessage.Builder builder = tx.clientMessage.toBuilder();
                // set the writeConcern for this particular transaction
                builder.setWriteConcern(s.consistency);

                // check if we have upgraded the consistency of this particular transaction
                if (s.consistency > tx.minRequiredConsistency) {
                    transactionsUpgraded++;
                }
                result.add(builder.build());
            }
            t = s.prevT;
        }

        Collections.reverse(result);
        // token cost is scaled down
        double tokensUsed = bestT / scale;

        return new ProcessResult(result, tokensUsed, bestProfit, transactionsUpgraded);
    }

    /**
     * First executes all transactions at minimum consistency, then upgrades to maximize profit
     * This is a hybrid approach that ensures throughput first, then optimizes for profit
     * 
     * Algorithm:
     * Phase 1: Execute ALL transactions at their minimum required consistency level
     * Phase 2: Only if ALL transactions were processed, use remaining tokens to upgrade
     *          transactions up to majority level to maximize profit
     */
    public ProcessResult processForThroughputThenProfit(List<TransactionOption> transactions, double currentTokens, int minTransactions, boolean allowUpgrades) {
        int n = transactions.size();
        int maxTokens = (int) Math.ceil(currentTokens * scale);
        int majorityLevel = (NUM_OF_SERVERS / 2) + 1;

        // Phase 1: Execute all transactions at their minimum required consistency
        List<Integer> assignedConsistency = new ArrayList<>();
        double usedTokens = 0.0;
        double profit = 0.0;
        List<ClientMessage> result = new ArrayList<>();

        // Sort by minimum consistency first, then by profit
        Collections.sort(transactions, (a, b) -> {
            if (a.minRequiredConsistency == b.minRequiredConsistency) {
                return Double.compare(b.baseProfit, a.baseProfit);
            }
            return a.minRequiredConsistency - b.minRequiredConsistency;
        });

        // Execute all transactions at minimum consistency
        boolean allTransactionsProcessed = true;
        for (int i = 0; i < n; i++) {
            TransactionOption tx = transactions.get(i);
            int minWC = tx.minRequiredConsistency;
            double cost = tokenCost(minWC);

            if (usedTokens + cost <= maxTokens) {
                usedTokens += cost;
                profit += tx.baseProfit;
                assignedConsistency.add(minWC);
            } else {
                // Can't afford this transaction, stop here
                assignedConsistency.add(-1); // Mark as not executed
                allTransactionsProcessed = false;
            }
        }

        // Phase 2: Upgrade transactions using remaining tokens to maximize profit
        // Only upgrade if ALL transactions were successfully processed in Phase 1
        double remainingTokens = maxTokens - usedTokens;
        
        if (allTransactionsProcessed && remainingTokens > 0 && allowUpgrades) {
            // Group upgrade options by transaction index
            // For each transaction, store all possible upgrades
            List<List<UpgradeOption>> upgradesByTransaction = new ArrayList<>();
            
            for (int i = 0; i < assignedConsistency.size(); i++) {
                int currentWC = assignedConsistency.get(i);
                if (currentWC == -1) continue; // Transaction not executed
                
                TransactionOption tx = transactions.get(i);
                List<UpgradeOption> txUpgrades = new ArrayList<>();
                
                // Try upgrading to all possible higher write concerns (up to majority level)
                // Note: currentWC == tx.minRequiredConsistency (from Phase 1)
                for (int newWC : writeConcernCosts.keySet()) {
                    if (newWC <= currentWC) continue; // Only upgrades
                    if (newWC > majorityLevel) continue; // Don't upgrade beyond majority
                    
                    double additionalCost = tokenCost(newWC) - tokenCost(currentWC);
                    if (additionalCost <= 0) continue; // Skip if no additional cost
                    
                    double additionalProfit = 0.0;
                    
                    // Calculate the additional profit from upgrading
                    // Case 1: Upgrading to majority level
                    if (newWC == majorityLevel) {
                        additionalProfit = tx.extraMajorityProfit;
                    } 
                    // Case 2: Upgrading to intermediate level (between min and majority)
                    else if (newWC > tx.minRequiredConsistency && newWC < majorityLevel) {
                        additionalProfit = tx.extraIntermediateProfit * (newWC - tx.minRequiredConsistency);
                    }
                    
                    if (additionalProfit > 0) {
                        txUpgrades.add(new UpgradeOption(i, currentWC, newWC, additionalCost, additionalProfit));
                    }
                }
                
                // Add this transaction's upgrade options (empty list if no upgrades possible)
                if (!txUpgrades.isEmpty()) {
                    upgradesByTransaction.add(txUpgrades);
                }
            }
            
            // Use DP to maximize profit: for each transaction, choose at most one upgrade
            int numTransactions = upgradesByTransaction.size();
            int maxUpgradeTokens = (int) Math.ceil(remainingTokens);
            
            if (numTransactions > 0 && maxUpgradeTokens > 0) {
                // dp[i][t] = maximum profit considering first i transactions with t tokens
                double[][] dp = new double[numTransactions + 1][maxUpgradeTokens + 1];
                
                // choice[i][t] = index of chosen upgrade for transaction i with t tokens (-1 = no upgrade)
                int[][] choice = new int[numTransactions + 1][maxUpgradeTokens + 1];
                
                // Initialize DP table
                for (int i = 0; i <= numTransactions; i++) {
                    for (int t = 0; t <= maxUpgradeTokens; t++) {
                        dp[i][t] = 0.0;
                        choice[i][t] = -1; // -1 means no upgrade
                    }
                }
                
                // Fill DP table
                for (int i = 1; i <= numTransactions; i++) {
                    List<UpgradeOption> txUpgrades = upgradesByTransaction.get(i - 1);
                    
                    for (int t = 0; t <= maxUpgradeTokens; t++) {
                        // Option 1: Don't upgrade this transaction
                        dp[i][t] = dp[i - 1][t];
                        choice[i][t] = -1;
                        
                        // Option 2: Try each possible upgrade for this transaction
                        for (int j = 0; j < txUpgrades.size(); j++) {
                            UpgradeOption upgrade = txUpgrades.get(j);
                            int cost = (int) Math.ceil(upgrade.additionalCost);
                            
                            if (t >= cost) {
                                double profitWithUpgrade = dp[i - 1][t - cost] + upgrade.additionalProfit;
                                if (profitWithUpgrade > dp[i][t]) {
                                    dp[i][t] = profitWithUpgrade;
                                    choice[i][t] = j; // Store which upgrade was chosen
                                }
                            }
                        }
                    }
                }
                
                // Find the token amount that gives maximum profit
                double maxProfit = 0.0;
                int bestTokenAmount = 0;
                
                for (int t = 0; t <= maxUpgradeTokens; t++) {
                    if (dp[numTransactions][t] > maxProfit) {
                        maxProfit = dp[numTransactions][t];
                        bestTokenAmount = t;
                    }
                }
                
                // Backtrack to find which upgrades to apply
                int t = bestTokenAmount;
                for (int i = numTransactions; i > 0; i--) {
                    int chosenUpgradeIndex = choice[i][t];
                    
                    if (chosenUpgradeIndex != -1) {
                        // Apply this upgrade
                        UpgradeOption upgrade = upgradesByTransaction.get(i - 1).get(chosenUpgradeIndex);
                        int cost = (int) Math.ceil(upgrade.additionalCost);
                        
                        assignedConsistency.set(upgrade.txIndex, upgrade.toWC);
                        usedTokens += upgrade.additionalCost;
                        profit += upgrade.additionalProfit;
                        
                        t -= cost;
                    }
                }
            }
        }

        // Build result with final consistency levels
        int transactionsUpgraded = 0;
        for (int i = 0; i < assignedConsistency.size(); i++) {
            int wc = assignedConsistency.get(i);
            if (wc == -1) continue; // Skip non-executed transactions
            
            TransactionOption tx = transactions.get(i);
            ClientMessage.Builder builder = tx.clientMessage.toBuilder();
            builder.setWriteConcern(wc);
            result.add(builder.build());
            
            if (wc > tx.minRequiredConsistency) {
                transactionsUpgraded++;
            }
        }

        return new ProcessResult(result, usedTokens / scale, profit, transactionsUpgraded);
    }

    /**
     * Helper class to represent an upgrade opportunity
     */
    private static class UpgradeOption {
        int txIndex;
        int fromWC;
        int toWC;
        double additionalCost;
        double additionalProfit;

        UpgradeOption(int txIndex, int fromWC, int toWC, double additionalCost, double additionalProfit) {
            this.txIndex = txIndex;
            this.fromWC = fromWC;
            this.toWC = toWC;
            this.additionalCost = additionalCost;
            this.additionalProfit = additionalProfit;
        }
    }

    /**
     * All the token costs are stored in a map, we use scale to convert them into integer, as we can't store double in DP
     */
    private double tokenCost(int consistency) {
        return Math.ceil(writeConcernCosts.getOrDefault(consistency, 0.0));
    }
}
