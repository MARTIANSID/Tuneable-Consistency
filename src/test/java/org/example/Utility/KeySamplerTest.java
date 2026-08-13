package org.example.Utility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;

import org.junit.jupiter.api.Test;

class KeySamplerTest {

    @Test
    void uniformCoversTheWholeKeySpace() {
        KeySampler sampler = KeySampler.uniform(10);
        Random random = new Random(42);
        int[] counts = new int[10];
        for (int i = 0; i < 10_000; i++) {
            counts[sampler.next(random)]++;
        }
        for (int k = 0; k < 10; k++) {
            assertTrue(counts[k] > 700 && counts[k] < 1300,
                    "uniform draw of key " + k + " out of expected band: " + counts[k]);
        }
    }

    @Test
    void zipfianSkewsTowardLowRanks() {
        KeySampler sampler = KeySampler.zipfian(100, 1.0);
        Random random = new Random(42);
        int[] counts = new int[100];
        for (int i = 0; i < 100_000; i++) {
            counts[sampler.next(random)]++;
        }
        // Rank 0 has weight 1/H_100 ~ 0.193; rank 99 ~ 0.00193. Allow slack.
        assertTrue(counts[0] > 15_000, "hottest key drew " + counts[0]);
        assertTrue(counts[0] > 10 * counts[99], "skew too shallow: " + counts[0] + " vs " + counts[99]);
        // Coarse monotonicity: the top decile outdraws the bottom decile.
        int top = 0, bottom = 0;
        for (int k = 0; k < 10; k++) {
            top += counts[k];
            bottom += counts[90 + k];
        }
        assertTrue(top > 5 * bottom, "top decile " + top + " vs bottom decile " + bottom);
    }

    @Test
    void zipfianExponentZeroIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> KeySampler.zipfian(10, 0.0));
        assertThrows(IllegalArgumentException.class, () -> KeySampler.uniform(0));
    }

    @Test
    void drawsStayInRange() {
        KeySampler sampler = KeySampler.zipfian(3, 2.0);
        Random random = new Random(7);
        for (int i = 0; i < 1_000; i++) {
            int key = sampler.next(random);
            assertTrue(key >= 0 && key < 3);
        }
        assertEquals(3, sampler.keySpace());
    }
}
