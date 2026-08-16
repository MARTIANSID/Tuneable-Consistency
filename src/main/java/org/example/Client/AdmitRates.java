package org.example.Client;

/**
 * Per-node admission tracker for one session client: exponentially decayed
 * admit/reject counts with a small prior.
 *
 * pAdmit(node) = (admits + PRIOR) / (admits + rejects + PRIOR)
 *
 * The prior makes the estimate 1.0 with no evidence (cold start is neutral)
 * and keeps it strictly above 0 under any number of rejections, so a
 * rejecting node always retains a re-probe incentive. decay() runs once per
 * second from the session's sweeper thread, forgetting old evidence so a
 * recovered node earns its traffic back. The session keeps one instance per
 * registered SLA (read and write separately), so admission pressure is
 * tracked per (node, SLA): the scorer rejects low-profit SLAs sooner, and a
 * node shedding browses may still be accepting checkouts.
 */
final class AdmitRates {

    private static final double PRIOR = 1.0;

    private final double[] admits;
    private final double[] rejects;
    private final double gamma;

    AdmitRates(int numServers, double gamma) {
        this.admits = new double[numServers];
        this.rejects = new double[numServers];
        this.gamma = gamma;
    }

    synchronized void onAdmit(int node) {
        admits[node] += 1;
    }

    synchronized void onReject(int node) {
        rejects[node] += 1;
    }

    /** Called on the session's 1 s sweeper tick; gamma is per second. */
    synchronized void decay() {
        for (int i = 0; i < admits.length; i++) {
            admits[i] *= gamma;
            rejects[i] *= gamma;
        }
    }

    synchronized double pAdmit(int node) {
        return (admits[node] + PRIOR) / (admits[node] + rejects[node] + PRIOR);
    }
}
