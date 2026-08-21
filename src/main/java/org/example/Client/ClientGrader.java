package org.example.Client;

import java.util.List;

import org.example.Utility.Grading;
import org.example.Utility.RungScorer;
import org.example.raft.KvResponse;
import org.example.raft.ReadLevel;

/**
 * The client-side SLA check: the one grader that feeds the ledger in every
 * mode. Responses carry both views; each view is graded on the step 7 ladder
 * from the returned state and session anchors, the best-paying rung across
 * both views wins (that view's value is what the application sees), and total
 * time is the client-observed end-to-end latency - the real SLA clock, not
 * the server's approximation.
 *
 * Causal labels reported by the server are diagnostic only. The client
 * independently establishes causal guarantees from the returned view
 * frontiers and its causal session anchor using the same grading rules in every
 * mode. Linearizable is the sole exception: a completed ReadIndex round
 * cannot be reconstructed from response indices, so the delivered
 * linearizable label is the proof of that mechanism.
 *
 * A deadline violation is a successful response for which at least one SLA
 * rung's consistency is satisfied but every applicable rung's end-to-end
 * deadline is missed.
 */
public final class ClientGrader {

    private ClientGrader() {
    }

    public record Verdict(
            /** Strongest ladder level the surfaced result satisfies. */
            int gradedStrength,
            /** Index of the best rung met in the SLA's rung list; -1 = none. */
            int satisfiedRung,
            double realizedProfit,
            /** True when the local view's value is the one surfaced. */
            boolean viaLocalView,
            boolean deadlineViolation) {
    }

    public static Verdict gradeRead(List<RungScorer.Rung> sla, KvResponse response,
            int uncommittedAnchor, double latencyMs) {
        // Do not seed either grade from a server-reported causal label: that
        // would let Chameleon grade its own work while Pileus is checked from
        // returned state. Keep Pileus's existing post-hoc Grading rules and
        // apply those exact rules to Chameleon responses too.
        int localGrade = Grading.gradeRead(ReadLevel.EVENTUAL_LOCAL,
                response.getLocalValueIndex(), response.getCommitIndex(), response.getLogIndex(),
                uncommittedAnchor);
        boolean linearizableCoversCausalFrontier = uncommittedAnchor < 0
                || response.getCommitIndex() >= uncommittedAnchor;
        ReadLevel committedBase = response.getDeliveredReadLevel() == ReadLevel.LINEARIZABLE
                && linearizableCoversCausalFrontier
                ? ReadLevel.LINEARIZABLE
                : ReadLevel.EVENTUAL_MAJORITY;
        int committedGrade = Grading.gradeRead(committedBase,
                response.getCommittedValueIndex(), response.getCommitIndex(), response.getCommitIndex(),
                uncommittedAnchor);

        Grading.Realized viaLocal = Grading.realize(sla, localGrade, latencyMs);
        Grading.Realized viaCommitted = Grading.realize(sla, committedGrade, latencyMs);
        // Ties go to the committed view: the safer value at equal pay.
        boolean localWins = viaLocal.profit() > viaCommitted.profit();
        Grading.Realized best = localWins ? viaLocal : viaCommitted;
        int gradedStrength = localWins ? localGrade : committedGrade;

        boolean deadlineViolation = best.rungIndex() < 0
                && (Grading.missedDeadline(sla, localGrade, latencyMs)
                        || Grading.missedDeadline(sla, committedGrade, latencyMs));
        return new Verdict(gradedStrength, best.rungIndex(), best.profit(), localWins, deadlineViolation);
    }

    public static Verdict gradeWrite(List<RungScorer.Rung> sla, KvResponse response, double latencyMs) {
        Grading.Realized realized = Grading.realize(sla, response.getDeliveredWriteConcern(), latencyMs);
        return new Verdict(response.getDeliveredWriteConcern(), realized.rungIndex(), realized.profit(),
                false, Grading.missedDeadline(sla, response.getDeliveredWriteConcern(), latencyMs));
    }
}
