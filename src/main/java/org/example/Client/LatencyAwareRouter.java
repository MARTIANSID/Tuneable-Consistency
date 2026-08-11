package org.example.Client;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.example.Utility.ExperimentConfig;

/**
 * Pileus-style latency-aware client routing: for each read, pick the
 * (node, consistency level) pair with the highest expected profit
 * P(success) * profit, where P(success) is computed from an empirical
 * distribution of base latencies.
 *
 * Estimator (see design discussion):
 *  - Per node: a FIFO sliding window of base latencies - samples from reads
 *    that EXECUTED at the lowest level (R:EVENTUAL). Kept sorted for O(log K)
 *    probability queries; eviction is strictly oldest-first so the window
 *    tracks the current latency regime without value bias.
 *  - Per (node, chosenLevel): an EWMA delta between observed latency and the
 *    node's base-window median at resolution time. Keyed by the CHOSEN level
 *    (the decision), so in hybrid mode (server upgrades on) the estimate
 *    marginalizes over the server's upgrade behavior. Delta for the lowest
 *    level is 0 by definition.
 *  - P(success) for a candidate = fraction of base-window samples b_i with
 *    b_i + delta <= deadline. Empty window or unseen delta are treated
 *    optimistically so every candidate gets tried once; epsilon-greedy
 *    exploration keeps estimates fresh thereafter.
 *
 * The router only chooses reads. Writes always go to the leader with their
 * phase-distribution write concern.
 */
public final class LatencyAwareRouter {

    private LatencyAwareRouter() {
    }

    /** Consistency levels in increasing strength order (ordinal = rank). */
    public enum Level {
        EVENTUAL,
        CAUSAL_LOCAL,
        CAUSAL_MAJORITY,
        LINEARIZABLE
    }

    public static final class Choice {
        public final int nodeId;
        public final Level level;

        Choice(int nodeId, Level level) {
            this.nodeId = nodeId;
            this.level = level;
        }
    }

    private static volatile boolean enabled = false;
    private static volatile double ewmaAlpha = 0.2;
    private static volatile double explorationRate = 0.05;
    private static volatile Map<Integer, Integer> deadlinesMsByApp = Map.of();
    private static volatile BaseWindow[] windows = new BaseWindow[0];
    // (nodeId | level) -> EWMA of (observed latency - base median), decision-keyed
    private static final ConcurrentHashMap<String, Double> deltas = new ConcurrentHashMap<>();
    private static final Random random = new Random();

    public static synchronized void configure(ExperimentConfig config) {
        enabled = config.clientRouting.mode.equals("LATENCY_AWARE");
        ewmaAlpha = config.clientRouting.ewmaAlpha;
        explorationRate = config.clientRouting.explorationRate;

        Map<Integer, Integer> deadlines = new ConcurrentHashMap<>();
        for (Map.Entry<String, Integer> e : config.clientMetrics.deadlinesMsByApp.entrySet()) {
            deadlines.put(Integer.parseInt(e.getKey()), e.getValue());
        }
        deadlinesMsByApp = deadlines;

        BaseWindow[] w = new BaseWindow[config.cluster.numServers];
        for (int i = 0; i < w.length; i++) {
            w[i] = new BaseWindow(config.clientRouting.baseWindowSize);
        }
        windows = w;
        deltas.clear();
    }

    public static boolean isEnabled() {
        return enabled;
    }

    // ------------------------------------------------------------------
    // Decision
    // ------------------------------------------------------------------

    /**
     * Choose (node, level) for a read with the given minimum level. The floor
     * is never violated downward; LINEARIZABLE is only legal on the leader.
     */
    public static Choice chooseRead(Level floor, int appId, int leaderId) {
        Integer deadline = deadlinesMsByApp.get(appId);
        if (deadline == null) {
            throw new IllegalStateException("No latency deadline configured for applicationId " + appId
                    + " (clientMetrics.deadlinesMsByApp)");
        }
        int numServers = windows.length;

        // Epsilon-greedy exploration: uniform random legal candidate.
        if (random.nextDouble() < explorationRate) {
            Level[] levels = Level.values();
            Level level = levels[floor.ordinal() + random.nextInt(levels.length - floor.ordinal())];
            int node = (level == Level.LINEARIZABLE) ? leaderId : random.nextInt(numServers);
            return new Choice(node, level);
        }

        Choice best = null;
        double bestExpectedProfit = -1;
        double bestEstimate = Double.MAX_VALUE;
        // Fallback (all-zero probabilities): minimum level, smallest estimate.
        int fallbackNode = leaderId;
        double fallbackEstimate = Double.MAX_VALUE;

        for (Level level : Level.values()) {
            if (level.ordinal() < floor.ordinal()) {
                continue;
            }
            int profitMultiplier = level.ordinal() + 1;
            for (int node = 0; node < numServers; node++) {
                if (level == Level.LINEARIZABLE && node != leaderId) {
                    continue;
                }
                double delta = (level == Level.EVENTUAL) ? 0.0
                        : deltas.getOrDefault(deltaKey(node, level), 0.0); // unseen -> optimistic
                BaseWindow window = windows[node];
                double p = window.fractionAtMost(deadline - delta); // empty window -> optimistic 1.0
                double median = window.medianOrZero();
                double estimate = median + delta;

                if (level == floor && estimate < fallbackEstimate) {
                    fallbackEstimate = estimate;
                    fallbackNode = node;
                }

                double expectedProfit = p * profitMultiplier * appId;
                if (expectedProfit > bestExpectedProfit
                        || (expectedProfit == bestExpectedProfit && estimate < bestEstimate)) {
                    bestExpectedProfit = expectedProfit;
                    bestEstimate = estimate;
                    best = new Choice(node, level);
                }
            }
        }

        if (best == null || bestExpectedProfit <= 0) {
            // Nothing is expected to make its deadline: serve at the minimum
            // required level on the fastest node rather than dropping.
            return new Choice((floor == Level.LINEARIZABLE) ? leaderId : fallbackNode, floor);
        }
        return best;
    }

    // ------------------------------------------------------------------
    // Feedback (called from ClientMetricsTracker on every resolved read)
    // ------------------------------------------------------------------

    static void onSample(int nodeId, boolean isRead, String chosenLevelLabel, String executedLevelLabel,
            long latencyMs) {
        if (!enabled || !isRead || nodeId < 0 || nodeId >= windows.length) {
            return;
        }
        BaseWindow window = windows[nodeId];

        // Base samples: only reads that actually EXECUTED at the lowest level.
        if ("R:EVENTUAL".equals(executedLevelLabel)) {
            window.add(latencyMs);
        }

        // Delta samples: decision-keyed, relative to the node's current base median.
        if (!"R:EVENTUAL".equals(chosenLevelLabel)) {
            Level chosen = levelFromLabel(chosenLevelLabel);
            if (chosen == null) {
                return; // not a read label
            }
            double median = window.medianOrNaN();
            if (Double.isNaN(median)) {
                return; // no base data yet for this node
            }
            double sample = latencyMs - median;
            deltas.merge(deltaKey(nodeId, chosen), sample,
                    (old, s) -> old + ewmaAlpha * (s - old));
        }
    }

    private static String deltaKey(int nodeId, Level level) {
        return nodeId + "|" + level;
    }

    private static Level levelFromLabel(String label) {
        return switch (label) {
            case "R:EVENTUAL" -> Level.EVENTUAL;
            case "R:CAUSAL_LOCAL" -> Level.CAUSAL_LOCAL;
            case "R:CAUSAL_MAJORITY" -> Level.CAUSAL_MAJORITY;
            case "R:LINEARIZABLE" -> Level.LINEARIZABLE;
            default -> null;
        };
    }

    // ------------------------------------------------------------------
    // Sliding window of base latencies: FIFO eviction, sorted query view
    // ------------------------------------------------------------------

    static final class BaseWindow {
        private final long[] ring;    // arrival order; next points at the oldest slot once full
        private final long[] sorted;  // same values, ascending
        private int count;
        private int next;

        BaseWindow(int capacity) {
            this.ring = new long[capacity];
            this.sorted = new long[capacity];
        }

        synchronized void add(long value) {
            if (count == ring.length) {
                removeSorted(ring[next]); // strictly oldest-first eviction; decrements count
            }
            insertSorted(value, count);   // insert among the current `count` elements
            count++;
            ring[next] = value;
            next = (next + 1) % ring.length;
        }

        /** Fraction of samples <= bound; optimistic 1.0 when empty (cold start). */
        synchronized double fractionAtMost(double bound) {
            if (count == 0) {
                return 1.0;
            }
            int lo = 0, hi = count;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (sorted[mid] <= bound) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            return (double) lo / count;
        }

        synchronized double medianOrNaN() {
            return (count == 0) ? Double.NaN : sorted[(count - 1) / 2];
        }

        synchronized double medianOrZero() {
            return (count == 0) ? 0.0 : sorted[(count - 1) / 2];
        }

        private void removeSorted(long value) {
            int lo = 0, hi = count - 1, idx = -1;
            while (lo <= hi) {
                int mid = (lo + hi) >>> 1;
                if (sorted[mid] == value) {
                    idx = mid;
                    break;
                } else if (sorted[mid] < value) {
                    lo = mid + 1;
                } else {
                    hi = mid - 1;
                }
            }
            System.arraycopy(sorted, idx + 1, sorted, idx, count - idx - 1);
            count--;
        }

        private void insertSorted(long value, int currentCount) {
            int lo = 0, hi = currentCount;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (sorted[mid] < value) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            System.arraycopy(sorted, lo, sorted, lo + 1, currentCount - lo);
            sorted[lo] = value;
        }
    }
}
