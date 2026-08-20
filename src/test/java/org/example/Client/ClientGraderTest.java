package org.example.Client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.example.Utility.RungScorer;
import org.example.raft.KvResponse;
import org.example.raft.ReadLevel;
import org.junit.jupiter.api.Test;

/**
 * The client-side SLA check: both views are graded, the best-paying rung
 * wins, and successful responses missing all applicable deadlines are marked.
 */
class ClientGraderTest {

    private static final List<RungScorer.Rung> SLA = List.of(
            new RungScorer.Rung(ReadLevel.CAUSAL_MAJORITY.getNumber(), 200, 6),
            new RungScorer.Rung(ReadLevel.CAUSAL_LOCAL.getNumber(), 100, 3),
            new RungScorer.Rung(ReadLevel.EVENTUAL_LOCAL.getNumber(), 100, 1));

    private static KvResponse.Builder response(ReadLevel delivered) {
        return KvResponse.newBuilder().setOk(true).setDeliveredReadLevel(delivered);
    }

    @Test
    void committedViewWinsWhenItPaysTheTopRung() {
        // Commit index covers the committed anchor, both values committed:
        // the CM rung (profit 6) pays via the committed view.
        KvResponse r = response(ReadLevel.EVENTUAL_MAJORITY)
                .setLogIndex(10).setCommitIndex(10)
                .setLocalValueIndex(9).setCommittedValueIndex(9)
                .build();
        ClientGrader.Verdict verdict = ClientGrader.gradeRead(SLA, r, 8, 8, 50);
        assertEquals(0, verdict.satisfiedRung());
        assertEquals(6.0, verdict.realizedProfit(), 1e-9);
        assertFalse(verdict.viaLocalView());
        assertFalse(verdict.deadlineViolation());
    }

    @Test
    void localViewWinsWhenOnlyItSatisfiesTheCausalClaim() {
        // The commit index trails both anchors, so the committed view can
        // only pay the EL rung; the local frontier covers the uncommitted
        // anchor, so causal-local pays 3 via the local view.
        KvResponse r = response(ReadLevel.EVENTUAL_LOCAL)
                .setLogIndex(12).setCommitIndex(5)
                .setLocalValueIndex(11).setCommittedValueIndex(4)
                .build();
        ClientGrader.Verdict verdict = ClientGrader.gradeRead(SLA, r, 10, 10, 50);
        assertEquals(1, verdict.satisfiedRung());
        assertEquals(3.0, verdict.realizedProfit(), 1e-9);
        assertTrue(verdict.viaLocalView());
        assertFalse(verdict.deadlineViolation());
    }

    @Test
    void latencyBlowingEveryThresholdRealizesNothing() {
        KvResponse r = response(ReadLevel.EVENTUAL_MAJORITY)
                .setLogIndex(10).setCommitIndex(10)
                .setLocalValueIndex(9).setCommittedValueIndex(9)
                .build();
        ClientGrader.Verdict verdict = ClientGrader.gradeRead(SLA, r, -1, -1, 500);
        assertEquals(-1, verdict.satisfiedRung());
        assertEquals(0.0, verdict.realizedProfit(), 1e-9);
        assertTrue(verdict.deadlineViolation());
    }

    @Test
    void writesGradeOnTheDeliveredConcern() {
        List<RungScorer.Rung> writeSla = List.of(
                new RungScorer.Rung(2, 300, 8),
                new RungScorer.Rung(1, 150, 3));
        KvResponse ack = KvResponse.newBuilder().setOk(true).setDeliveredWriteConcern(2).build();
        ClientGrader.Verdict verdict = ClientGrader.gradeWrite(writeSla, ack, 100);
        assertEquals(0, verdict.satisfiedRung());
        assertEquals(8.0, verdict.realizedProfit(), 1e-9);
        assertFalse(verdict.deadlineViolation());
        ClientGrader.Verdict late = ClientGrader.gradeWrite(writeSla, ack, 400);
        assertEquals(-1, late.satisfiedRung());
        assertTrue(late.deadlineViolation());
    }
}
