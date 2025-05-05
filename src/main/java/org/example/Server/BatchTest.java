package org.example.Server;

import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.ArrayList;
import java.util.AbstractMap;
import java.util.Collections;
public class BatchTest {
    // Reuse your inner class
    private static class QueuedTransaction {
        String id;
        int minRequiredConsistency;
        double baseProfit;
        double extraProfitOnMajority;
        public QueuedTransaction(String id, int minReq, double baseProfit, double extraProfit) {
            this.id = id;
            this.minRequiredConsistency = minReq;
            this.baseProfit = baseProfit;
            this.extraProfitOnMajority = extraProfit;
        }
    }

    public static void main(String[] args) {
        // 1) Build our example batch
        List<QueuedTransaction> batch = List.of(
            new QueuedTransaction("t1", 1, 1, 1),
            new QueuedTransaction("t2", 1, 1, 2),
            new QueuedTransaction("t3", 2, 2, 1),
            new QueuedTransaction("t4", 1, 3, 0),
            new QueuedTransaction("t5", 1, 1, 2)
        );

        double rawTokens = 5;               // available tokens
        int maxUnits   = (int) Math.floor(rawTokens * 2); // scale×2 → integer units
        int R          = 4;                 // target throughput
        int n          = batch.size();

        // build cost & profit arrays
        int[] costMin = new int[n], costMaj = new int[n];
        double[] profMin = new double[n], profMaj = new double[n];
        for (int i = 0; i < n; i++) {
            QueuedTransaction qt = batch.get(i);
            costMin[i] = (qt.minRequiredConsistency == 1 ? 1 : 2);
            costMaj[i] = 2;
            profMin[i] = qt.baseProfit;
            profMaj[i] = qt.baseProfit + qt.extraProfitOnMajority;
        }

        // DP tables
        double[][][] dp = new double[n+1][n+1][maxUnits+1];
        int[][][] choice = new int[n+1][n+1][maxUnits+1];

        // init
        for (int i = 0; i <= n; i++)
            for (int j = 0; j <= n; j++)
                Arrays.fill(dp[i][j], Double.NEGATIVE_INFINITY);
        dp[0][0][0] = 0;

        // fill DP
        for (int i = 1; i <= n; i++) {
            for (int j = 0; j <= i; j++) {
                for (int w = 0; w <= maxUnits; w++) {
                    // skip
                    dp[i][j][w] = dp[i-1][j][w];
                    choice[i][j][w] = 0;
                    // take at min
                    if (j>0 && w>=costMin[i-1] && dp[i-1][j-1][w-costMin[i-1]]>-1e9) {
                        double cand = dp[i-1][j-1][w-costMin[i-1]] + profMin[i-1];
                        if (cand > dp[i][j][w]) {
                            dp[i][j][w] = cand;
                            choice[i][j][w] = 1;
                        }
                    }
                    // take as majority
                    if (batch.get(i-1).minRequiredConsistency==1 
                     && j>0 && w>=costMaj[i-1] && dp[i-1][j-1][w-costMaj[i-1]]>-1e9) {
                        double cand = dp[i-1][j-1][w-costMaj[i-1]] + profMaj[i-1];
                        if (cand > dp[i][j][w]) {
                            dp[i][j][w] = cand;
                            choice[i][j][w] = 2;
                        }
                    }
                }
            }
        }

        // pick best j ≥ R
        double bestProfit = Double.NEGATIVE_INFINITY;
        int bestJ = R, bestW = 0;
        for (int j = R; j <= n; j++) {
            for (int w = 0; w <= maxUnits; w++) {
                if (dp[n][j][w] > bestProfit) {
                    bestProfit = dp[n][j][w];
                    bestJ = j;
                    bestW = w;
                }
            }
        }

        // backtrack
        List<Map.Entry<QueuedTransaction,Integer>> chosen = new ArrayList<>();
        int i=n, j=bestJ, w=bestW;
        while (i>0) {
            int ch = choice[i][j][w];
            if (ch==1||ch==2) {
                QueuedTransaction qt = batch.get(i-1);
                chosen.add(new AbstractMap.SimpleEntry<>(qt, ch==1?qt.minRequiredConsistency:2));
                w -= (ch==1?costMin[i-1]:costMaj[i-1]);
                j--;
            }
            i--;
        }
        Collections.reverse(chosen);

        long upgrades = chosen.stream().filter(e->e.getValue()==2).count();
        double tokensUsed = bestW / 2.0;
        double tokensLeft = rawTokens - tokensUsed;

        // print summary
        System.out.printf(
            "📦 BatchSummary: picked %d/%d (upgrades=%d), profit=%.1f, tokensUsed=%.1f→left=%.1f%n",
            chosen.size(), n, upgrades, bestProfit, tokensUsed, tokensLeft
        );
    }
}
