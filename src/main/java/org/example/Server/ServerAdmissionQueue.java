package org.example.Server;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A service-wide bounded admission executor. Framed requests are independent,
 * so the shared worker pool can schedule each request directly while one global
 * permit budget bounds aggregate queued and executing work.
 */
final class ServerAdmissionQueue implements AutoCloseable {

    private final Semaphore permits;
    private final ExecutorService workers;
    private final ServerDrainMetrics metrics;
    private final Object lifecycle = new Object();
    private boolean closed;
    private int outstandingTasks;

    ServerAdmissionQueue(int capacity, int workerCount, int nodeId, ServerDrainMetrics metrics) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("admission queue capacity must be positive, got " + capacity);
        }
        if (workerCount <= 0) {
            throw new IllegalArgumentException("admission worker count must be positive, got " + workerCount);
        }
        this.permits = new Semaphore(capacity);
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        AtomicInteger threadNumber = new AtomicInteger();
        ThreadFactory factory = task -> {
            Thread thread = new Thread(task,
                    "kv-admission-" + nodeId + "-" + threadNumber.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };
        this.workers = Executors.newFixedThreadPool(workerCount, factory);
    }

    /**
     * Enqueue work without blocking the ingress event loop. False means the global
     * bounded queue is full; executor shutdown is an explicit error.
     */
    boolean trySubmit(Runnable task) {
        Objects.requireNonNull(task, "task");
        if (!permits.tryAcquire()) {
            return false;
        }

        long enqueuedNanos = System.nanoTime();
        metrics.admissionQueued();
        try {
            enqueue(() -> {
                long startedNanos = System.nanoTime();
                metrics.admissionWorkStarted(startedNanos - enqueuedNanos);
                try {
                    task.run();
                } finally {
                    metrics.admissionWorkCompleted(System.nanoTime() - startedNanos);
                    permits.release();
                }
            });
            metrics.admissionQueueAccepted();
            return true;
        } catch (RejectedExecutionException e) {
            metrics.admissionQueueSubmissionCancelled();
            permits.release();
            throw e;
        }
    }

    int availablePermits() {
        return permits.availablePermits();
    }

    private void enqueue(Runnable task) {
        synchronized (lifecycle) {
            if (closed) {
                throw new RejectedExecutionException("admission queue is closed");
            }
            outstandingTasks++;
            try {
                workers.execute(() -> runTask(task));
            } catch (RejectedExecutionException e) {
                outstandingTasks--;
                throw e;
            }
        }
    }

    private void runTask(Runnable task) {
        try {
            task.run();
        } finally {
            synchronized (lifecycle) {
                outstandingTasks--;
                if (outstandingTasks == 0) {
                    lifecycle.notifyAll();
                }
            }
        }
    }

    @Override
    public void close() {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        boolean interrupted = false;
        synchronized (lifecycle) {
            closed = true;
            try {
                while (outstandingTasks > 0) {
                    long remainingNanos = deadlineNanos - System.nanoTime();
                    if (remainingNanos <= 0) {
                        break;
                    }
                    TimeUnit.NANOSECONDS.timedWait(lifecycle, remainingNanos);
                }
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        workers.shutdown();
        try {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0 || !workers.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)) {
                workers.shutdownNow();
            }
        } catch (InterruptedException e) {
            interrupted = true;
            workers.shutdownNow();
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
