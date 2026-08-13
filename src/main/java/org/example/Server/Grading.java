package org.example.Server;

import java.util.List;

import org.example.raft.ReadLevel;

/**
 * Step 7: grade what was actually delivered, not what was targeted. Reads
 * frequently satisfy a stronger level than the one chosen, and that profit is
 * free.
 *
 * The graded strength is the strongest level on the ladder whose own
 * requirement the delivered result meets, never below the executed level
 * (whose mechanics guaranteed it):
 *
 * - eventual-majority: the returned value's index is at or below the commit
 *   index, so the value is majority-committed (a committed-view read
 *   satisfies this by construction).
 * - causal-local: the view that served the value had reached the session's
 *   uncommitted anchor, so the session's acknowledged writes were visible.
 * - causal-majority: the value is committed and the commit index had reached
 *   the session's committed anchor.
 * - linearizable: never grantable after the fact; it requires the leadership
 *   confirmation round that only the executed path performs.
 *
 * Realized profit is the highest-profit rung whose consistency requirement
 * the graded strength covers and whose threshold the total time, service time
 * plus rho, came in under. Writes cannot be upgraded this way (once
 * acknowledged the decision is spent), so their graded strength is simply the
 * replication count at acknowledgment.
 */
public final class Grading {

    private Grading() {
    }

    /** The best rung met: its index in the SLA's registered rung list, and its profit. */
    public record Realized(int rungIndex, double profit) {
        public static final Realized NONE = new Realized(-1, 0.0);
    }

    /**
     * Grade a read. {@code viewFrontierIndex} is the index frontier of the
     * view that served the value: the last log index for local-view levels,
     * the commit index for committed-view levels. Indices are the node's at
     * grading time; anchors are the request's (-1 = none).
     */
    public static int gradeRead(ReadLevel executed, int valueIndex, int commitIndex, int viewFrontierIndex,
            int uncommittedAnchor, int committedAnchor) {
        int graded = executed.getNumber();
        boolean valueCommitted = valueIndex <= commitIndex;
        if (valueCommitted) {
            graded = Math.max(graded, ReadLevel.EVENTUAL_MAJORITY.getNumber());
        }
        if (uncommittedAnchor < 0 || viewFrontierIndex >= uncommittedAnchor) {
            graded = Math.max(graded, ReadLevel.CAUSAL_LOCAL.getNumber());
        }
        if (valueCommitted && (committedAnchor < 0 || commitIndex >= committedAnchor)) {
            graded = Math.max(graded, ReadLevel.CAUSAL_MAJORITY.getNumber());
        }
        return graded;
    }

    /**
     * The highest-profit rung whose requirement {@code gradedStrength} covers
     * and whose threshold {@code totalMs} (service time plus rho) came in
     * under. Ties on profit go to the earliest registered rung.
     */
    public static Realized realize(List<RungScorer.Rung> slaRungs, int gradedStrength, double totalMs) {
        int bestIndex = -1;
        double bestProfit = 0.0;
        for (int i = 0; i < slaRungs.size(); i++) {
            RungScorer.Rung rung = slaRungs.get(i);
            if (gradedStrength >= rung.strength() && totalMs <= rung.thresholdMs()
                    && rung.profit() > bestProfit) {
                bestIndex = i;
                bestProfit = rung.profit();
            }
        }
        return bestIndex < 0 ? Realized.NONE : new Realized(bestIndex, bestProfit);
    }
}
