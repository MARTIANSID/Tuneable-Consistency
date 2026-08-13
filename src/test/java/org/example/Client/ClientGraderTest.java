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
 * The client-side SLA check: both views graded, the best-paying rung wins,
 * session assertions ride whichever view makes a causal claim.
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
        ClientGrader.Verdict verdict = ClientGrader.gradeRead(SLA, r, 8, 8, null, null, 50);
        assertEquals(0, verdict.satisfiedRung());
        assertEquals(6.0, verdict.realizedProfit(), 1e-9);
        assertFalse(verdict.viaLocalView());
        assertFalse(verdict.violation());
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
        ClientGrader.Verdict verdict = ClientGrader.gradeRead(SLA, r, 10, 10, 10, null, 50);
        assertEquals(1, verdict.satisfiedRung());
        assertEquals(3.0, verdict.realizedProfit(), 1e-9);
        assertTrue(verdict.viaLocalView());
        assertFalse(verdict.violation());
    }

    @Test
    void latencyBlowingEveryThresholdRealizesNothing() {
        KvResponse r = response(ReadLevel.EVENTUAL_MAJORITY)
                .setLogIndex(10).setCommitIndex(10)
                .setLocalValueIndex(9).setCommittedValueIndex(9)
                .build();
        ClientGrader.Verdict verdict = ClientGrader.gradeRead(SLA, r, -1, -1, null, null, 500);
        assertEquals(-1, verdict.satisfiedRung());
        assertEquals(0.0, verdict.realizedProfit(), 1e-9);
    }

    @Test
    void causalClaimsAreAssertedAgainstTheClaimingView() {
        // The local frontier covers the anchor (a causal-local claim) but the
        // local value predates this session's write to the key: violation.
        KvResponse stale = response(ReadLevel.EVENTUAL_LOCAL)
                .setLogIndex(20).setCommitIndex(3)
                .setLocalValueIndex(2).setCommittedValueIndex(2)
                .build();
        assertTrue(ClientGrader.gradeRead(SLA, stale, 10, -1, 10, null, 50).violation());

        // Same shape on the committed side: commit index covers the anchor
        // but the committed value predates the majority-acked write.
        KvResponse staleCommitted = response(ReadLevel.EVENTUAL_MAJORITY)
                .setLogIndex(20).setCommitIndex(20)
                .setLocalValueIndex(15).setCommittedValueIndex(2)
                .build();
        assertTrue(ClientGrader.gradeRead(SLA, staleCommitted, -1, 10, null, 10, 50).violation());
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
        assertEquals(-1, ClientGrader.gradeWrite(writeSla, ack, 400).satisfiedRung());
    }
}
