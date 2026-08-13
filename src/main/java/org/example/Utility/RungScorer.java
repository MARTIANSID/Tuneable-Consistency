package org.example.Utility;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.DoubleUnaryOperator;

/**
 * Step 4 of the request path: score one candidate level against an SLA's
 * rungs. Pure functions over (rungs, level strength, rho, F_c, omega_c,
 * lambda) so the arithmetic is unit-testable against the plan's worked
 * example.
 *
 * A rung is (kappa, delta, pi): consistency requirement expressed as a
 * comparable strength, end-to-end latency threshold in ms, and profit. A
 * level satisfies its own rungs and every weaker one (4a). Thresholds move to
 * the server's clock by subtracting the client's RTT estimate; rungs whose
 * budget the network alone consumed are discarded (4b). Survivors are sorted
 * ascending, identical thresholds collapse to one entry holding the suffix
 * maximum (4c/4d), and expected profit uses the by-parts form, which needs
 * only k CDF lookups (4e):
 *
 *   E_c = sum over i of (M_i - M_(i+1)) * F_c(d_(i))
 *
 * The score is V_c = E_c - lambda * omega_c (4f): expected profit minus the
 * price of the slot time the level consumes.
 */
public final class RungScorer {

    private RungScorer() {
    }

    /** One SLA rung with its consistency requirement as a comparable strength. */
    public record Rung(int strength, double thresholdMs, double profit) {
    }

    /** Result of scoring one level. */
    public record ScoredLevel(
            /** E_c: expected profit of running this level. */
            double expectedProfit,
            /** V_c = E_c - lambda * omega_c. */
            double value,
            /** Loosest surviving server-side threshold; bounds the step 6 wait. 0 if none survive. */
            double dMaxMs,
            /** Rungs this level satisfies before threshold conversion (4a). */
            int satisfiableRungs,
            /** Rungs remaining after discarding thresholds the network consumed (4b). */
            int survivingRungs) {
    }

    /**
     * Score level c. {@code cdf} is F_c of the histogram cell (level, gap
     * bucket) this request would run under; {@code omegaMs} is that cell's
     * running mean.
     */
    public static ScoredLevel score(List<Rung> slaRungs, int levelStrength, double rhoMs,
            DoubleUnaryOperator cdf, double omegaMs, double lambda) {
        // 4a: restrict to satisfiable rungs.
        List<Rung> satisfiable = new ArrayList<>();
        for (Rung rung : slaRungs) {
            if (levelStrength >= rung.strength()) {
                satisfiable.add(rung);
            }
        }

        // 4b: convert thresholds to the server's clock; discard spent budgets.
        List<double[]> survivors = new ArrayList<>(); // {d, profit}
        for (Rung rung : satisfiable) {
            double d = rung.thresholdMs() - rhoMs;
            if (d > 0) {
                survivors.add(new double[] { d, rung.profit() });
            }
        }
        if (survivors.isEmpty()) {
            return new ScoredLevel(0.0, -lambda * omegaMs, 0.0, satisfiable.size(), 0);
        }

        // 4c: sort ascending by threshold and collapse identical thresholds.
        survivors.sort(Comparator.comparingDouble(a -> a[0]));

        // 4d: suffix maximum of profits, collapsing duplicates as we go.
        int n = survivors.size();
        double[] thresholds = new double[n];
        double[] suffixMax = new double[n];
        int distinct = 0;
        for (int i = n - 1; i >= 0; i--) {
            double d = survivors.get(i)[0];
            double profit = survivors.get(i)[1];
            if (distinct > 0 && thresholds[distinct - 1] == d) {
                suffixMax[distinct - 1] = Math.max(suffixMax[distinct - 1], profit);
            } else {
                thresholds[distinct] = d;
                suffixMax[distinct] = (distinct > 0) ? Math.max(suffixMax[distinct - 1], profit) : profit;
                distinct++;
            }
        }
        // thresholds[0..distinct) is now DESCENDING with suffixMax aligned;
        // walk it back to ascending order for the by-parts sum.

        // 4e: by-parts expected profit.
        double expected = 0.0;
        for (int i = distinct - 1; i >= 0; i--) {
            double mNext = (i > 0) ? suffixMax[i - 1] : 0.0; // M_(i+1) in ascending order
            expected += (suffixMax[i] - mNext) * cdf.applyAsDouble(thresholds[i]);
        }

        // 4f: pay for the capacity the level consumes.
        double value = expected - lambda * omegaMs;
        double dMax = thresholds[0]; // largest threshold (descending order head)
        return new ScoredLevel(expected, value, dMax, satisfiable.size(), survivors.size());
    }
}
