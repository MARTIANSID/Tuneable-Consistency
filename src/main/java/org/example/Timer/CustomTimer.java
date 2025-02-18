package org.example.Timer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class CustomTimer {
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Runnable task;
    private final long timeout;
    private ScheduledFuture<?> future;

    public CustomTimer(Runnable task, long timeout, TimeUnit unit) {
        this.task = task;
        this.timeout = unit.toMillis(timeout);
    }

    public synchronized void start() {
        if (future == null || future.isCancelled() || future.isDone()) {
            future = scheduler.schedule(task, timeout, TimeUnit.MILLISECONDS);
        }
    }

    public synchronized void stop() {
        if (future != null && !future.isDone()) {
            future.cancel(false);
        }
    }
    public synchronized boolean isRunning() {
        return future != null && !future.isDone() && !future.isCancelled();
    }

    public synchronized void reset() {
        stop();
        start();
    }

    public void shutdown() {
        scheduler.shutdown();
    }
}
