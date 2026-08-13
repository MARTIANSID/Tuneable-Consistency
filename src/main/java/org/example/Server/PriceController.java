package org.example.Server;

/**
 * The shadow price controller: once per control interval,
 *
 *   lambda <- max(lambda_min, lambda * exp(eta * (u - u_target)))
 *
 * Multiplicative because the right price spans orders of magnitude and can
 * never go negative; a controller rather than a closed form because profit
 * units are whatever the applications say they are. The floor does two jobs:
 * it lets lambda start at zero (the multiplicative update alone would leave
 * zero unchanged forever) and it stops lambda decaying so far during idle
 * periods that recovery takes many intervals.
 *
 * Requests never read u; they read the published lambda, which is up to one
 * interval stale by design.
 */
public final class PriceController {

    private final double uTarget;
    private final double eta;
    private final double lambdaMin;

    private volatile double lambda = 0.0;

    public PriceController(double uTarget, double eta, double lambdaMin) {
        this.uTarget = uTarget;
        this.eta = eta;
        this.lambdaMin = lambdaMin;
    }

    /** Called once per control interval with that interval's utilization. */
    public void update(double utilization) {
        lambda = Math.max(lambdaMin, lambda * Math.exp(eta * (utilization - uTarget)));
    }

    /** The published price; up to one control interval stale. */
    public double lambda() {
        return lambda;
    }

    /** Test-only: set the price directly to probe admission at a known lambda. */
    void forceLambda(double value) {
        lambda = value;
    }
}
