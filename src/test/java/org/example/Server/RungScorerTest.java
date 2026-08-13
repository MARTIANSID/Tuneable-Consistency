package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.function.DoubleUnaryOperator;

import org.example.Utility.RungScorer;
import org.junit.jupiter.api.Test;

/**
 * Pins the step 4 arithmetic to the plan's worked example: SLA rungs
 * (linearizable, 150, 10), (causal-majority, 150, 6), (causal-majority, 400,
 * 4), (eventual, 400, 1), (eventual, 150, 5), rho = 20. Expected profits are
 * 4.96, 5.76, 8.08; at lambda 0.02 linearizable wins, at 0.08 eventual wins.
 * Nothing switches modes; the price moves.
 */
class RungScorerTest {

    // Strengths for the example's three-level ladder.
    private static final int EVENTUAL = 0;
    private static final int CAUSAL_MAJORITY = 1;
    private static final int LINEARIZABLE = 2;

    private static final List<RungScorer.Rung> SLA = List.of(
            new RungScorer.Rung(LINEARIZABLE, 150, 10),
            new RungScorer.Rung(CAUSAL_MAJORITY, 150, 6),
            new RungScorer.Rung(CAUSAL_MAJORITY, 400, 4),
            new RungScorer.Rung(EVENTUAL, 400, 1),
            new RungScorer.Rung(EVENTUAL, 150, 5));

    private static final double RHO = 20;

    // F_c as step data at the two surviving thresholds (130 and 380).
    private static DoubleUnaryOperator cdf(double f130, double f380) {
        return x -> (x <= 130) ? f130 : f380;
    }

    private static final DoubleUnaryOperator F_EVENTUAL = cdf(0.99, 1.00);
    private static final DoubleUnaryOperator F_CM = cdf(0.90, 0.99);
    private static final DoubleUnaryOperator F_LIN = cdf(0.70, 0.97);

    @Test
    void expectedProfitsMatchTheWorkedExample() {
        assertEquals(4.96, RungScorer.score(SLA, EVENTUAL, RHO, F_EVENTUAL, 2, 0).expectedProfit(), 1e-9);
        assertEquals(5.76, RungScorer.score(SLA, CAUSAL_MAJORITY, RHO, F_CM, 40, 0).expectedProfit(), 1e-9);
        assertEquals(8.08, RungScorer.score(SLA, LINEARIZABLE, RHO, F_LIN, 80, 0).expectedProfit(), 1e-9);
    }

    @Test
    void linearizableWinsAtLowPriceEventualWinsAtHighPrice() {
        double lambda = 0.02;
        double vEventual = RungScorer.score(SLA, EVENTUAL, RHO, F_EVENTUAL, 2, lambda).value();
        double vCm = RungScorer.score(SLA, CAUSAL_MAJORITY, RHO, F_CM, 40, lambda).value();
        double vLin = RungScorer.score(SLA, LINEARIZABLE, RHO, F_LIN, 80, lambda).value();
        assertEquals(4.92, vEventual, 1e-9);
        assertEquals(4.96, vCm, 1e-9);
        assertEquals(6.48, vLin, 1e-9);
        assertTrue(vLin > vCm && vCm > vEventual, "linearizable must win at lambda 0.02");

        lambda = 0.08;
        vEventual = RungScorer.score(SLA, EVENTUAL, RHO, F_EVENTUAL, 2, lambda).value();
        vCm = RungScorer.score(SLA, CAUSAL_MAJORITY, RHO, F_CM, 40, lambda).value();
        vLin = RungScorer.score(SLA, LINEARIZABLE, RHO, F_LIN, 80, lambda).value();
        assertEquals(4.80, vEventual, 1e-9);
        assertEquals(2.56, vCm, 1e-9);
        assertEquals(1.68, vLin, 1e-9);
        assertTrue(vEventual > vCm && vCm > vLin, "eventual must win at lambda 0.08");
    }

    @Test
    void dMaxIsTheLoosestSurvivingThreshold() {
        assertEquals(380, RungScorer.score(SLA, EVENTUAL, RHO, F_EVENTUAL, 2, 0).dMaxMs(), 1e-9);
        assertEquals(380, RungScorer.score(SLA, LINEARIZABLE, RHO, F_LIN, 80, 0).dMaxMs(), 1e-9);
    }

    @Test
    void networkConsumedBudgetsAreDiscarded() {
        // rho = 200 kills the 150 ms rungs; only the 400 ms rungs survive.
        RungScorer.ScoredLevel scored = RungScorer.score(SLA, LINEARIZABLE, 200, F_LIN, 80, 0);
        assertEquals(2, scored.survivingRungs());
        assertEquals(200, scored.dMaxMs(), 1e-9);
        // M over the surviving 200 ms threshold = max(4, 1) = 4.
        assertEquals(4 * 0.97, scored.expectedProfit(), 1e-9);

        // rho beyond every threshold: nothing survives, expected profit is
        // zero and only the capacity cost remains.
        RungScorer.ScoredLevel spent = RungScorer.score(SLA, LINEARIZABLE, 500, F_LIN, 80, 0.02);
        assertEquals(0, spent.survivingRungs());
        assertEquals(0.0, spent.expectedProfit(), 1e-9);
        assertEquals(-0.02 * 80, spent.value(), 1e-9);
    }

    @Test
    void levelsSatisfyOnlyTheirOwnRungsAndWeaker() {
        // Eventual can never claim the causal-majority or linearizable rungs.
        RungScorer.ScoredLevel eventual = RungScorer.score(SLA, EVENTUAL, RHO, x -> 1.0, 0, 0);
        assertEquals(2, eventual.satisfiableRungs());
        // A level below every rung has nothing.
        RungScorer.ScoredLevel below = RungScorer.score(
                List.of(new RungScorer.Rung(LINEARIZABLE, 100, 5)), EVENTUAL, 0, x -> 1.0, 0, 0);
        assertEquals(0, below.satisfiableRungs());
        assertEquals(0.0, below.expectedProfit(), 1e-9);
    }

    @Test
    void looserRungPayingMoreIsHandledBySuffixMax() {
        // A looser rung that pays more than a tighter one: the suffix max
        // makes the tight threshold worth the looser rung's profit too.
        List<RungScorer.Rung> odd = List.of(
                new RungScorer.Rung(EVENTUAL, 100, 2),
                new RungScorer.Rung(EVENTUAL, 300, 9));
        // F = 1 everywhere: E must be the best single claimable profit, 9.
        assertEquals(9.0, RungScorer.score(odd, EVENTUAL, 0, x -> 1.0, 0, 0).expectedProfit(), 1e-9);
    }
}
