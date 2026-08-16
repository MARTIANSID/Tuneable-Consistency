package org.example.Client;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.example.Utility.RungScorer;
import org.example.raft.ReadLevel;

/**
 * The Pileus client: picks (contact server, target rung) to maximize expected
 * profit, confirmed after the fact by grading. Per candidate the expectation
 * is profit x P(consistency feasible at that server) x P(latency within the
 * rung's threshold):
 *
 * - Consistency is binary from tracked per-server high-water indices (every
 *   response reports the server's log and commit index) against the session's
 *   anchors; eventual rungs are always feasible, linearizable is structural
 *   (leader, or any server when follower LIN reads are enabled) because the
 *   mechanism guarantees the outcome - all its uncertainty is latency.
 * - Latency comes from 2n + wc response-time sliding windows: per-server
 *   plain reads, per-server linearizable reads (the confirmation round and
 *   catch-up wait are part of what must be predicted), and per-concern
 *   writes (writes always go to the leader). Empty windows read as feasible
 *   (cold-start optimism), and timed-out attempts land at-or-beyond the
 *   threshold so they count themselves as misses.
 *
 * A small exploration fraction routes to a random server so windows and
 * staleness estimates never freeze on stale samples. Ties break to the
 * weakest requirement, mirroring the server scorer.
 */
final class PileusSelector {

    private final int numServers;
    private final int majority;
    private final SlidingWindow[] plainWindows;
    private final SlidingWindow[] linWindows;
    private final SlidingWindow[] writeWindows; // index = concern - 1
    private final AtomicIntegerArray highLogIndex;
    private final AtomicIntegerArray highCommitIndex;
    private final boolean followerLinReads;
    private final double explorationFraction;
    private final Random random;
    // Admission-aware routing (null when disabled): expected profit is
    // multiplied by the node's decayed admit probability, so a rejecting node
    // loses attractiveness in profit space instead of through latency-window
    // penalty samples.
    private final AdmitRates admitRates;

    record Choice(int node, int rungIndex, double expectedProfit) {
    }

    PileusSelector(int numServers, int majority, int windowSize, boolean followerLinReads,
            double explorationFraction, Random random, AdmitRates admitRates) {
        this.numServers = numServers;
        this.majority = majority;
        this.followerLinReads = followerLinReads;
        this.explorationFraction = explorationFraction;
        this.random = random;
        this.admitRates = admitRates;
        this.plainWindows = new SlidingWindow[numServers];
        this.linWindows = new SlidingWindow[numServers];
        for (int i = 0; i < numServers; i++) {
            plainWindows[i] = new SlidingWindow(windowSize);
            linWindows[i] = new SlidingWindow(windowSize);
        }
        this.writeWindows = new SlidingWindow[majority];
        for (int c = 0; c < majority; c++) {
            writeWindows[c] = new SlidingWindow(windowSize);
        }
        this.highLogIndex = new AtomicIntegerArray(numServers);
        this.highCommitIndex = new AtomicIntegerArray(numServers);
        for (int i = 0; i < numServers; i++) {
            highLogIndex.set(i, -1);
            highCommitIndex.set(i, -1);
        }
    }

    // ===== Observation =====

    void observeIndices(int node, int logIndex, int commitIndex) {
        highLogIndex.accumulateAndGet(node, logIndex, Math::max);
        highCommitIndex.accumulateAndGet(node, commitIndex, Math::max);
    }

    void observeRead(int node, boolean linTargeted, double latencyMs) {
        (linTargeted ? linWindows : plainWindows)[node].add(latencyMs);
    }

    void observeWrite(int concern, double latencyMs) {
        writeWindows[Math.min(Math.max(1, concern), majority) - 1].add(latencyMs);
    }

    // A rejection carries no latency, but it must not be invisible: an empty
    // window reads as cold-start optimistic, so a node rejecting under its
    // occupancy cap would otherwise stay the most attractive target forever.
    // A penalty sample beyond every threshold makes the window reflect the
    // rejection; exploration re-probes the node once it recovers.
    private static final double REJECTION_PENALTY_MS = 1e9;

    void observeReadRejected(int node, boolean linTargeted) {
        observeRead(node, linTargeted, REJECTION_PENALTY_MS);
    }

    void observeWriteRejected(int concern) {
        observeWrite(concern, REJECTION_PENALTY_MS);
    }

    // ===== Selection =====

    Choice chooseRead(List<RungScorer.Rung> sla, int uncommittedAnchor, int committedAnchor, int leaderHint) {
        if (explorationFraction > 0 && random.nextDouble() < explorationFraction) {
            int node = random.nextInt(numServers);
            return bestRungForNode(sla, node, uncommittedAnchor, committedAnchor, leaderHint);
        }
        Choice best = null;
        for (int node = 0; node < numServers; node++) {
            Choice candidate = bestRungForNode(sla, node, uncommittedAnchor, committedAnchor, leaderHint);
            if (candidate == null) {
                continue;
            }
            if (best == null || candidate.expectedProfit() > best.expectedProfit()) {
                best = candidate;
            }
        }
        return best;
    }

    /** The best rung to target at one specific server (used for exploration too). */
    private Choice bestRungForNode(List<RungScorer.Rung> sla, int node, int uncommittedAnchor,
            int committedAnchor, int leaderHint) {
        double admitFactor = admitRates == null ? 1.0 : admitRates.pAdmit(node);
        Choice best = null;
        int bestStrength = Integer.MAX_VALUE;
        for (int i = 0; i < sla.size(); i++) {
            RungScorer.Rung rung = sla.get(i);
            boolean lin = rung.strength() == ReadLevel.LINEARIZABLE.getNumber();
            if (lin && !followerLinReads && leaderHint >= 0 && node != leaderHint) {
                continue; // LIN is structurally impossible off-leader
            }
            double feasible = readFeasible(rung.strength(), node, uncommittedAnchor, committedAnchor) ? 1.0 : 0.0;
            SlidingWindow window = lin ? linWindows[node] : plainWindows[node];
            double expected = rung.profit() * feasible * window.fractionAtMost(rung.thresholdMs()) * admitFactor;
            // Strictly-greater keeps the weakest requirement on ties.
            if (best == null || expected > best.expectedProfit()
                    || (expected == best.expectedProfit() && rung.strength() < bestStrength)) {
                best = new Choice(node, i, expected);
                bestStrength = rung.strength();
            }
        }
        return best;
    }

    private boolean readFeasible(int strength, int node, int uncommittedAnchor, int committedAnchor) {
        if (strength == ReadLevel.CAUSAL_LOCAL.getNumber()) {
            return uncommittedAnchor < 0 || highLogIndex.get(node) >= uncommittedAnchor;
        }
        if (strength == ReadLevel.CAUSAL_MAJORITY.getNumber()) {
            return committedAnchor < 0 || highCommitIndex.get(node) >= committedAnchor;
        }
        // Eventual levels always; linearizable by mechanism where it may run.
        return true;
    }

    /** Writes always target the leader; only the concern is chosen. */
    Choice chooseWrite(List<RungScorer.Rung> sla, int leaderNode) {
        Choice best = null;
        int bestConcern = Integer.MAX_VALUE;
        for (int i = 0; i < sla.size(); i++) {
            RungScorer.Rung rung = sla.get(i);
            int concern = Math.min(Math.max(1, rung.strength()), majority);
            double expected = rung.profit() * writeWindows[concern - 1].fractionAtMost(rung.thresholdMs());
            if (best == null || expected > best.expectedProfit()
                    || (expected == best.expectedProfit() && concern < bestConcern)) {
                best = new Choice(leaderNode, i, expected);
                bestConcern = concern;
            }
        }
        return best;
    }
}
