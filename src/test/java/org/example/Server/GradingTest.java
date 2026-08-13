package org.example.Server;

import static org.example.raft.ReadLevel.CAUSAL_LOCAL;
import static org.example.raft.ReadLevel.CAUSAL_MAJORITY;
import static org.example.raft.ReadLevel.EVENTUAL_LOCAL;
import static org.example.raft.ReadLevel.EVENTUAL_MAJORITY;
import static org.example.raft.ReadLevel.LINEARIZABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

/** Step 7 grading: the plan's two upgrade cases plus the realized-profit rules. */
class GradingTest {

    // ===== gradeRead =====

    @Test
    void valueAtOrBelowCommitIndexUpgradesLocalToMajority() {
        // Executed eventual-local, but the returned value's write is already
        // majority-committed. Anchors block the causal levels, so the upgrade
        // is exactly local -> majority.
        int graded = Grading.gradeRead(EVENTUAL_LOCAL, 5, 7, 6, 8, 9);
        assertEquals(EVENTUAL_MAJORITY.getNumber(), graded);
    }

    @Test
    void replicaAtOrAheadOfSessionIndexUpgradesEventualToCausal() {
        // Executed eventual-local on a replica whose log frontier covers the
        // session's uncommitted anchor: causal-local is satisfied even though
        // eventual was executed. The value itself is uncommitted, so the
        // majority levels are not.
        int graded = Grading.gradeRead(EVENTUAL_LOCAL, 9, 7, 10, 8, 9);
        assertEquals(CAUSAL_LOCAL.getNumber(), graded);
    }

    @Test
    void committedValueOnACaughtUpReplicaGradesCausalMajority() {
        int graded = Grading.gradeRead(EVENTUAL_LOCAL, 5, 7, 9, 6, 7);
        assertEquals(CAUSAL_MAJORITY.getNumber(), graded);
    }

    @Test
    void noSessionHistoryMakesCausalVacuouslySatisfied() {
        // -1 anchors: the session never wrote, so causal holds trivially; a
        // committed value grades causal-majority.
        assertEquals(CAUSAL_MAJORITY.getNumber(), Grading.gradeRead(EVENTUAL_MAJORITY, 5, 7, 7, -1, -1));
        // An uncommitted value from the local view still grades causal-local.
        assertEquals(CAUSAL_LOCAL.getNumber(), Grading.gradeRead(EVENTUAL_LOCAL, 9, 7, 9, -1, -1));
    }

    @Test
    void linearizableIsNeverGrantedAfterTheFact() {
        assertEquals(CAUSAL_MAJORITY.getNumber(), Grading.gradeRead(CAUSAL_MAJORITY, 5, 7, 7, -1, -1));
    }

    @Test
    void executedLinearizableKeepsItsStrength() {
        assertEquals(LINEARIZABLE.getNumber(), Grading.gradeRead(LINEARIZABLE, 5, 7, 7, -1, -1));
    }

    @Test
    void gradingNeverDemotesTheExecutedLevel() {
        // Anchors ahead of everything: no upgrade condition holds, but the
        // executed level's mechanics already delivered it.
        int graded = Grading.gradeRead(CAUSAL_MAJORITY, 9, 7, 7, 20, 20);
        assertEquals(CAUSAL_MAJORITY.getNumber(), graded);
    }

    @Test
    void absentKeyIsTriviallyCommitted() {
        // valueIndex -1 (key absent): both views agree, so majority holds.
        assertEquals(CAUSAL_MAJORITY.getNumber(), Grading.gradeRead(EVENTUAL_LOCAL, -1, 7, 9, -1, -1));
    }

    // ===== realize =====

    private static final List<RungScorer.Rung> SLA = List.of(
            new RungScorer.Rung(LINEARIZABLE.getNumber(), 300, 10),
            new RungScorer.Rung(CAUSAL_MAJORITY.getNumber(), 150, 6),
            new RungScorer.Rung(EVENTUAL_MAJORITY.getNumber(), 100, 2));

    @Test
    void realizeTakesTheHighestProfitRungMetOnBothAxes() {
        // Delivered causal-majority in 120 ms total: the LIN rung fails on
        // consistency, the CM rung passes both, the EM rung is worth less.
        Grading.Realized realized = Grading.realize(SLA, CAUSAL_MAJORITY.getNumber(), 120);
        assertEquals(1, realized.rungIndex());
        assertEquals(6, realized.profit());
    }

    @Test
    void realizeChecksTotalTimeAgainstEachRungsOwnThreshold() {
        // 200 ms total: CM's 150 ms threshold is blown, EM's 100 ms as well;
        // only the LIN rung's 300 ms survives but its consistency is not met.
        assertEquals(Grading.Realized.NONE, Grading.realize(SLA, CAUSAL_MAJORITY.getNumber(), 200));
        // Delivered linearizable: the LIN rung pays.
        Grading.Realized lin = Grading.realize(SLA, LINEARIZABLE.getNumber(), 200);
        assertEquals(0, lin.rungIndex());
        assertEquals(10, lin.profit());
    }

    @Test
    void realizeReturnsNoneWhenNothingIsMet() {
        assertEquals(Grading.Realized.NONE, Grading.realize(SLA, EVENTUAL_LOCAL.getNumber(), 50));
        assertEquals(Grading.Realized.NONE, Grading.realize(SLA, LINEARIZABLE.getNumber(), 500));
    }

    @Test
    void writeGradingUsesTheDeliveredConcern() {
        List<RungScorer.Rung> writeSla = List.of(
                new RungScorer.Rung(2, 300, 8),
                new RungScorer.Rung(1, 150, 3));
        assertEquals(8, Grading.realize(writeSla, 2, 250).profit());
        assertEquals(3, Grading.realize(writeSla, 1, 100).profit());
        assertEquals(Grading.Realized.NONE, Grading.realize(writeSla, 1, 200));
    }
}
