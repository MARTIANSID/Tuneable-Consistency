package org.example.Client;

import java.util.List;

import org.example.Utility.Grading;
import org.example.Utility.RungScorer;
import org.example.raft.KvResponse;
import org.example.raft.ReadLevel;

/**
 * The client-side SLA check: the one grader that feeds the ledger in every
 * mode. Responses carry both views; each view is graded on the step 7 ladder
 * starting from the level the server's mechanics actually guaranteed for it,
 * the best-paying rung across both views wins (that view's value is what the
 * application sees), and total time is the client-observed end-to-end
 * latency - the real SLA clock, not the server's approximation.
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
            int uncommittedAnchor, int committedAnchor, double latencyMs) {
        ReadLevel delivered = response.getDeliveredReadLevel();

        // Each view's grade starts from what the executed mechanics
        // guaranteed for that view and rises per the ladder conditions.
        ReadLevel localBase = delivered == ReadLevel.CAUSAL_LOCAL ? ReadLevel.CAUSAL_LOCAL
                : ReadLevel.EVENTUAL_LOCAL;
        ReadLevel committedBase = switch (delivered) {
            case LINEARIZABLE -> ReadLevel.LINEARIZABLE;
            case CAUSAL_MAJORITY -> ReadLevel.CAUSAL_MAJORITY;
            default -> ReadLevel.EVENTUAL_MAJORITY;
        };
        int localGrade = Grading.gradeRead(localBase, response.getLocalValueIndex(),
                response.getCommitIndex(), response.getLogIndex(), uncommittedAnchor, committedAnchor);
        int committedGrade = Grading.gradeRead(committedBase, response.getCommittedValueIndex(),
                response.getCommitIndex(), response.getCommitIndex(), uncommittedAnchor, committedAnchor);

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
