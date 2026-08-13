package org.example.Utility;

import java.util.Random;

/**
 * Key selection for the workload: uniform or zipfian over [0, keySpace).
 * Zipfian weight of rank r (0-based) is 1 / (r + 1)^exponent; sampling walks
 * a precomputed cumulative distribution with binary search, so a draw is
 * O(log keySpace) with no rejection. Rank 0 is the hottest key.
 */
public final class KeySampler {

    private final int keySpace;
    private final double[] cumulative; // null = uniform

    private KeySampler(int keySpace, double[] cumulative) {
        this.keySpace = keySpace;
        this.cumulative = cumulative;
    }

    public static KeySampler uniform(int keySpace) {
        requireKeySpace(keySpace);
        return new KeySampler(keySpace, null);
    }

    public static KeySampler zipfian(int keySpace, double exponent) {
        requireKeySpace(keySpace);
        if (exponent <= 0) {
            throw new IllegalArgumentException("zipfian exponent must be > 0, got " + exponent);
        }
        double[] cumulative = new double[keySpace];
        double sum = 0;
        for (int r = 0; r < keySpace; r++) {
            sum += 1.0 / Math.pow(r + 1, exponent);
            cumulative[r] = sum;
        }
        for (int r = 0; r < keySpace; r++) {
            cumulative[r] /= sum;
        }
        return new KeySampler(keySpace, cumulative);
    }

    private static void requireKeySpace(int keySpace) {
        if (keySpace <= 0) {
            throw new IllegalArgumentException("keySpace must be > 0, got " + keySpace);
        }
    }

    /** Draw one key index in [0, keySpace). */
    public int next(Random random) {
        if (cumulative == null) {
            return random.nextInt(keySpace);
        }
        double u = random.nextDouble();
        int lo = 0, hi = keySpace - 1;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (cumulative[mid] < u) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    public int keySpace() {
        return keySpace;
    }
}
