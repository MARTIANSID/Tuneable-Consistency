package org.example.Client;

/**
 * Per-node RTT estimation (stage 5, protocol rho): RTT = end-to-end latency
 * minus the server-reported service time, sampled only from replies that
 * involved no server-side waiting, so the estimate never absorbs queueing or
 * index waits. The estimate is the median of a sliding window per node; empty
 * windows report 0 (optimistic cold start, consistent with the scorer's
 * free-and-certain rule for empty cells).
 */
public final class RttEstimator {

    private final SlidingWindow[] windows;

    public RttEstimator(int numNodes, int windowSize) {
        this.windows = new SlidingWindow[numNodes];
        for (int i = 0; i < numNodes; i++) {
            windows[i] = new SlidingWindow(windowSize);
        }
    }

    /**
     * Feed one reply. Waited replies are excluded: their latency is dominated
     * by the server-side wait, which is already inside the reported service
     * time on the server's clock but would double-count client-side skew, and
     * a negative difference (clock jitter) is discarded outright.
     */
    public void observe(int nodeId, double latencyMs, double serviceTimeMs, boolean waited) {
        if (waited) {
            return;
        }
        double rtt = latencyMs - serviceTimeMs;
        if (rtt >= 0) {
            windows[nodeId].add(rtt);
        }
    }

    /** The current estimate for a node; 0 until a sample arrives. */
    public double estimateMs(int nodeId) {
        return windows[nodeId].medianOr(0.0);
    }

    public int sampleCount(int nodeId) {
        return windows[nodeId].size();
    }
}
