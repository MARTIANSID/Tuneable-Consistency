package org.example.Client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class AdmitRatesTest {

    @Test
    void coldStartIsNeutralAndNeverZero() {
        AdmitRates rates = new AdmitRates(3, 0.9);
        assertEquals(1.0, rates.pAdmit(0));
        // Any number of rejections keeps pAdmit strictly above 0 (the prior),
        // so a rejecting node always retains a re-probe incentive.
        for (int i = 0; i < 1000; i++) {
            rates.onReject(0);
        }
        assertTrue(rates.pAdmit(0) > 0);
        assertTrue(rates.pAdmit(0) < 0.01);
        // Other nodes are untouched.
        assertEquals(1.0, rates.pAdmit(1));
    }

    @Test
    void ratioTracksAdmitsAndRejects() {
        AdmitRates rates = new AdmitRates(1, 0.9);
        for (int i = 0; i < 9; i++) {
            rates.onAdmit(0);
        }
        rates.onReject(0);
        // (9 + 1) / (9 + 1 + 1) with prior 1.
        assertEquals(10.0 / 11.0, rates.pAdmit(0), 1e-9);
    }

    @Test
    void decayForgetsRejections() {
        AdmitRates rates = new AdmitRates(1, 0.5);
        for (int i = 0; i < 100; i++) {
            rates.onReject(0);
        }
        double before = rates.pAdmit(0);
        for (int tick = 0; tick < 10; tick++) {
            rates.decay();
        }
        // 100 rejections decayed by 0.5^10 are ~0.1; pAdmit recovers toward 1.
        assertTrue(rates.pAdmit(0) > before);
        assertTrue(rates.pAdmit(0) > 0.9);
    }
}
