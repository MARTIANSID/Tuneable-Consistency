package org.example.Client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Random;

import org.example.Utility.RungScorer;
import org.example.raft.ReadLevel;
import org.junit.jupiter.api.Test;

class PileusSelectorTest {

    private static final int LIN = ReadLevel.LINEARIZABLE.getNumber();
    private static final int CM = ReadLevel.CAUSAL_MAJORITY.getNumber();
    private static final int EL = ReadLevel.EVENTUAL_LOCAL.getNumber();

    private static PileusSelector selector() {
        return new PileusSelector(3, 2, 16, false, 0.0, new Random(7));
    }

    @Test
    void coldStartIsOptimisticAndTiesBreakToTheWeakestRequirement() {
        // Empty windows read every candidate as certain, so expected profit
        // is just the rung profit; the LIN rung pays most but is only legal
        // on the leader; equal-profit ties go to the weaker requirement.
        PileusSelector selector = selector();
        List<RungScorer.Rung> sla = List.of(
                new RungScorer.Rung(LIN, 300, 10),
                new RungScorer.Rung(CM, 150, 6),
                new RungScorer.Rung(EL, 100, 6));
        PileusSelector.Choice choice = selector.chooseRead(sla, -1, -1, 1, null);
        assertEquals(1, choice.node(), "only the leader can serve the top-paying LIN rung");
        assertEquals(0, choice.rungIndex());
        assertEquals(10.0, choice.expectedProfit(), 1e-9);

        // Without the LIN rung, CM and EL tie at 6: the weaker EL rung wins.
        PileusSelector.Choice tie = selector.chooseRead(sla.subList(1, 3), -1, -1, 1, null);
        assertEquals(2, tie.rungIndex() + 1, "profit tie must resolve to the weakest requirement");
    }

    @Test
    void staleServersAreInfeasibleForCausalRungsUntilObserved() {
        PileusSelector selector = selector();
        List<RungScorer.Rung> sla = List.of(
                new RungScorer.Rung(CM, 150, 6),
                new RungScorer.Rung(EL, 100, 1));
        // The session's committed anchor is 50; no server has reported
        // reaching it, so the CM rung is infeasible everywhere and the EL
        // rung wins by default.
        assertEquals(1, selector.chooseRead(sla, -1, 50, 0, null).rungIndex());
        // One server reports commit index 60: CM becomes feasible there.
        selector.observeIndices(2, 70, 60);
        PileusSelector.Choice choice = selector.chooseRead(sla, -1, 50, 0, null);
        assertEquals(0, choice.rungIndex());
        assertEquals(2, choice.node());
    }

    @Test
    void latencyWindowsSteerAwayFromSlowTargets() {
        PileusSelector selector = selector();
        List<RungScorer.Rung> sla = List.of(
                new RungScorer.Rung(EL, 100, 5),
                new RungScorer.Rung(EL, 500, 2));
        // Server 0's plain reads blow the 100 ms threshold; server 1 is fast.
        for (int i = 0; i < 16; i++) {
            selector.observeRead(0, false, 400);
            selector.observeRead(1, false, 10);
        }
        // Server 2 stays cold (optimistic), so it ties with server 1 at full
        // certainty; either way the slow server 0 must lose the 100 ms rung.
        PileusSelector.Choice choice = selector.chooseRead(sla, -1, -1, 0, null);
        assertEquals(0, choice.rungIndex());
        assertEquals(5.0, choice.expectedProfit(), 1e-9);
        // On server 0 itself the 500 ms rung is the better target.
        for (int i = 0; i < 16; i++) {
            selector.observeRead(2, false, 400);
            selector.observeRead(1, false, 400);
        }
        assertEquals(1, selector.chooseRead(sla, -1, -1, 0, null).rungIndex(),
                "with every server slow, the loose rung is the best target");
    }

    @Test
    void writeConcernIsChosenByItsOwnWindow() {
        PileusSelector selector = selector();
        List<RungScorer.Rung> sla = List.of(
                new RungScorer.Rung(2, 100, 5),
                new RungScorer.Rung(1, 100, 2));
        // Cold: the wc:2 rung pays most.
        assertEquals(0, selector.chooseWrite(sla, 1).rungIndex());
        // wc:2 acks blow the threshold, wc:1 stays fast: the choice flips.
        for (int i = 0; i < 16; i++) {
            selector.observeWrite(2, 400);
            selector.observeWrite(1, 5);
        }
        PileusSelector.Choice choice = selector.chooseWrite(sla, 1);
        assertEquals(1, choice.rungIndex());
        assertEquals(1, choice.node(), "writes always target the leader");
    }
}
