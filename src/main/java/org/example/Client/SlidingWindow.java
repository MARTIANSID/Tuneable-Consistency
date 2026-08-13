package org.example.Client;

/**
 * FIFO sliding window with a sorted query view: O(log K) median and
 * fraction-below queries, strictly oldest-first eviction so the window tracks
 * the current regime without value bias. Extracted from the retired
 * latency-aware router; the client uses it to estimate per-node RTT from
 * no-wait replies (end-to-end latency minus reported service time).
 */
public final class SlidingWindow {
    private final double[] ring;    // arrival order; next points at the oldest slot once full
    private final double[] sorted;  // same values, ascending
    private int count;
    private int next;

    public SlidingWindow(int capacity) {
        this.ring = new double[capacity];
        this.sorted = new double[capacity];
    }

    public synchronized void add(double value) {
        if (count == ring.length) {
            removeSorted(ring[next]); // strictly oldest-first eviction; decrements count
        }
        insertSorted(value, count);   // insert among the current `count` elements
        count++;
        ring[next] = value;
        next = (next + 1) % ring.length;
    }

    /** Median of the window, or the given default when empty. */
    public synchronized double medianOr(double whenEmpty) {
        return (count == 0) ? whenEmpty : sorted[(count - 1) / 2];
    }

    /** Fraction of samples <= bound; optimistic 1.0 when empty (cold start). */
    public synchronized double fractionAtMost(double bound) {
        if (count == 0) {
            return 1.0;
        }
        int lo = 0, hi = count;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (sorted[mid] <= bound) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return (double) lo / count;
    }

    public synchronized int size() {
        return count;
    }

    private void removeSorted(double value) {
        int lo = 0, hi = count - 1, idx = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (sorted[mid] == value) {
                idx = mid;
                break;
            } else if (sorted[mid] < value) {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        System.arraycopy(sorted, idx + 1, sorted, idx, count - idx - 1);
        count--;
    }

    private void insertSorted(double value, int currentCount) {
        int lo = 0, hi = currentCount;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (sorted[mid] < value) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        System.arraycopy(sorted, lo, sorted, lo + 1, currentCount - lo);
        sorted[lo] = value;
    }
}
