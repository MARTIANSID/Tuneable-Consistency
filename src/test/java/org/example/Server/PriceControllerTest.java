package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PriceControllerTest {

    @Test
    void startsAtZeroAndTheFloorBootstrapsIt() {
        PriceController controller = new PriceController(0.85, 1.0, 0.0001);
        assertEquals(0.0, controller.lambda(), 1e-12, "initialized to zero");
        controller.update(0.1);
        assertEquals(0.0001, controller.lambda(), 1e-12,
                "the multiplicative update leaves zero unchanged; the floor bootstraps");
    }

    @Test
    void risesMultiplicativelyAboveTargetAndDecaysBelow() {
        PriceController controller = new PriceController(0.85, 1.0, 0.0001);
        controller.update(0.85); // bootstrap to the floor
        double previous = controller.lambda();

        for (int i = 0; i < 5; i++) {
            controller.update(1.85); // u - target = 1
            assertEquals(previous * Math.E, controller.lambda(), previous * 1e-9,
                    "each interval above target multiplies lambda by exp(eta * (u - target))");
            previous = controller.lambda();
        }

        for (int i = 0; i < 100; i++) {
            controller.update(0.0);
        }
        assertEquals(0.0001, controller.lambda(), 1e-12,
                "idle periods decay lambda to the floor, not to zero");
    }

    @Test
    void gainScalesTheResponse() {
        PriceController gentle = new PriceController(0.85, 0.1, 0.0001);
        PriceController aggressive = new PriceController(0.85, 2.0, 0.0001);
        gentle.update(1.85);
        aggressive.update(1.85);
        gentle.update(1.85);
        aggressive.update(1.85);
        assertTrue(aggressive.lambda() > gentle.lambda());
        assertEquals(0.0001 * Math.exp(0.1), gentle.lambda(), 1e-12);
        assertEquals(0.0001 * Math.exp(2.0), aggressive.lambda(), 1e-12);
    }
}
