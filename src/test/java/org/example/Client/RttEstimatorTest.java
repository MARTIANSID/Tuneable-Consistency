package org.example.Client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RttEstimatorTest {

    @Test
    void estimatesTheMedianOfLatencyMinusServiceTime() {
        RttEstimator estimator = new RttEstimator(2, 8);
        estimator.observe(0, 10.0, 4.0, false); // rtt 6
        estimator.observe(0, 12.0, 4.0, false); // rtt 8
        estimator.observe(0, 9.0, 5.0, false);  // rtt 4
        assertEquals(6.0, estimator.estimateMs(0), 1e-9);
        // The other node's window is independent.
        assertEquals(0.0, estimator.estimateMs(1), 1e-9);
    }

    @Test
    void waitedRepliesAreExcluded() {
        RttEstimator estimator = new RttEstimator(1, 8);
        estimator.observe(0, 10.0, 4.0, false);
        // A waited reply's latency is dominated by the server-side wait; it
        // must not move the estimate no matter how large it is.
        estimator.observe(0, 500.0, 480.0, true);
        estimator.observe(0, 500.0, 10.0, true);
        assertEquals(1, estimator.sampleCount(0));
        assertEquals(6.0, estimator.estimateMs(0), 1e-9);
    }

    @Test
    void negativeDifferencesAreDiscarded() {
        RttEstimator estimator = new RttEstimator(1, 8);
        estimator.observe(0, 4.0, 10.0, false); // clock jitter: latency < service time
        assertEquals(0, estimator.sampleCount(0));
        assertEquals(0.0, estimator.estimateMs(0), 1e-9);
    }

    @Test
    void reportsZeroUntilASampleArrives() {
        RttEstimator estimator = new RttEstimator(1, 8);
        assertEquals(0.0, estimator.estimateMs(0), 1e-9);
    }
}
