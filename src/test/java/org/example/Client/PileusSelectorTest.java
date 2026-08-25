package org.example.Client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void coldStartIsOptimisticAndTiesBreakByRungOrder() {
        // Empty windows read every candidate as certain, so expected profit
        // is just the rung profit; the LIN rung pays most but is only legal
        // on the leader; equal-profit ties resolve by registration order
        // (rungs are listed in decreasing preference, the paper's subSLA
        // ordering).
        PileusSelector selector = selector();
        List<RungScorer.Rung> sla = List.of(
                new RungScorer.Rung(LIN, 300, 10),
                new RungScorer.Rung(CM, 150, 6),
                new RungScorer.Rung(EL, 100, 6));
        PileusSelector.Choice choice = selector.chooseRead(sla, -1, 1, null);
        assertEquals(1, choice.node(), "only the leader can serve the top-paying LIN rung");
        assertEquals(0, choice.rungIndex());
        assertEquals(10.0, choice.expectedProfit(), 1e-9);

        // Without the LIN rung, CM and EL tie at 6: the earlier-listed
        // (more preferred) CM rung wins the tie.
        PileusSelector.Choice tie = selector.chooseRead(sla.subList(1, 3), -1, 1, null);
        assertEquals(0, tie.rungIndex(), "profit ties resolve by registration order");
    }

    @Test
    void allZeroExpectationsTargetTheMostPreferredRung() {
        // Overload despair: the causal rungs are infeasible (no server has
        // committed the anchor) and the LIN latency window is saturated far
        // beyond its threshold, so every rung scores exactly 0. The choice
        // must be the first-listed rung - under the old weakest-requirement
        // tie-break this degenerated to targeting an undeliverable CM rung.
        PileusSelector selector = new PileusSelector(3, 2, 16, true, 0.0, new Random(7));
        List<RungScorer.Rung> sla = List.of(
                new RungScorer.Rung(LIN, 300, 70),
                new RungScorer.Rung(CM, 300, 52),
                new RungScorer.Rung(CM, 1500, 30));
        for (int node = 0; node < 3; node++) {
            for (int i = 0; i < 16; i++) {
                selector.observeRead(node, true, 1e9);
            }
        }
        PileusSelector.Choice choice = selector.chooseRead(sla, 1000, 0, null);
        assertEquals(0, choice.rungIndex(), "despair must target the most preferred rung");
        assertEquals(0.0, choice.expectedProfit(), 1e-9);
    }

    @Test
    void infeasibleChoicesAreFlaggedForWaitSuppression() {
        // The anchor is beyond every observed commit index: the only rung is
        // causal-majority, so it wins by default but carries feasible=false,
        // which the session uses to suppress the doomed server-side wait.
        PileusSelector selector = selector();
        List<RungScorer.Rung> sla = List.of(new RungScorer.Rung(CM, 300, 52));
        PileusSelector.Choice choice = selector.chooseRead(sla, 1000, 0, null);
        assertEquals(0, choice.rungIndex());
        assertFalse(choice.feasible());
        // A server reports the anchor committed: the flag clears there.
        selector.observeIndices(1, 1200, 1100);
        assertTrue(selector.chooseRead(sla, 1000, 0, null).feasible());
    }

    @Test
    void deliveryFailuresDemoteATargetAndDecayRestoresIt() {
        // Served-below-target feedback: reads targeted at LIN keep grading
        // below it (wait-expiry fallbacks the latency window cannot see).
        // Expected profit must collapse, and decay must restore optimism so
        // the target can win its way back after the lag drains.
        PileusSelector selector = new PileusSelector(2, 2, 16, true, 0.0, new Random(7));
        List<RungScorer.Rung> sla = List.of(new RungScorer.Rung(LIN, 300, 70));
        for (int i = 0; i < 50; i++) {
            selector.observeReadDelivery(0, LIN, false);
            selector.observeReadDelivery(1, LIN, false);
        }
        double depressed = selector.chooseRead(sla, -1, -1, null).expectedProfit();
        assertTrue(depressed < 70 * 0.05, "delivery failures must collapse expected profit, got " + depressed);
        for (int i = 0; i < 200; i++) {
            selector.decayDeliveryRates(0.9);
        }
        assertEquals(70.0, selector.chooseRead(sla, -1, -1, null).expectedProfit(), 1.0);
    }

    @Test
    void staleServersAreInfeasibleForCausalRungsUntilObserved() {
        PileusSelector selector = selector();
        List<RungScorer.Rung> sla = List.of(
                new RungScorer.Rung(CM, 150, 6),
                new RungScorer.Rung(EL, 100, 1));
        // The session's latest acknowledged write is 50; no server has
        // reported committing it, so the CM rung is infeasible everywhere
        // and the EL rung wins by default.
        assertEquals(1, selector.chooseRead(sla, 50, 0, null).rungIndex());
        // One server reports commit index 60: CM becomes feasible there.
        selector.observeIndices(2, 70, 60);
        PileusSelector.Choice choice = selector.chooseRead(sla, 50, 0, null);
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
        PileusSelector.Choice choice = selector.chooseRead(sla, -1, 0, null);
        assertEquals(0, choice.rungIndex());
        assertEquals(5.0, choice.expectedProfit(), 1e-9);
        // On server 0 itself the 500 ms rung is the better target.
        for (int i = 0; i < 16; i++) {
            selector.observeRead(2, false, 400);
            selector.observeRead(1, false, 400);
        }
        assertEquals(1, selector.chooseRead(sla, -1, 0, null).rungIndex(),
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
