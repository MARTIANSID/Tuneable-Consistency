package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class ServerAdmissionQueueTest {

    @Test
    void rejectsWithoutBlockingWhenTheGlobalPermitBudgetIsFull() throws Exception {
        ServerDrainMetrics metrics = new ServerDrainMetrics(98);
        try (ServerAdmissionQueue queue = new ServerAdmissionQueue(1, 1, 98, metrics)) {
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            CountDownLatch completed = new CountDownLatch(1);

            assertTrue(queue.trySubmit(() -> {
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    completed.countDown();
                }
            }));
            assertTrue(started.await(2, TimeUnit.SECONDS));
            assertEquals(0, queue.availablePermits());
            assertFalse(queue.trySubmit(() -> {
                throw new AssertionError("full queue must not execute rejected work");
            }));

            release.countDown();
            assertTrue(completed.await(2, TimeUnit.SECONDS));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (queue.availablePermits() != 1 && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertEquals(1, queue.availablePermits());
        }
    }

    @Test
    void independentFramedRequestsUseTheSharedWorkers() throws Exception {
        ServerDrainMetrics metrics = new ServerDrainMetrics(100);
        try (ServerAdmissionQueue queue = new ServerAdmissionQueue(4, 2, 100, metrics)) {
            CountDownLatch slowStarted = new CountDownLatch(1);
            CountDownLatch releaseSlow = new CountDownLatch(1);
            CountDownLatch independentCompleted = new CountDownLatch(1);

            assertTrue(queue.trySubmit(() -> {
                slowStarted.countDown();
                try {
                    releaseSlow.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
            assertTrue(slowStarted.await(2, TimeUnit.SECONDS));

            try {
                assertTrue(queue.trySubmit(independentCompleted::countDown));
                assertTrue(independentCompleted.await(2, TimeUnit.SECONDS),
                        "an independent framed request must use the other shared worker");
            } finally {
                releaseSlow.countDown();
            }
        }
    }
}
