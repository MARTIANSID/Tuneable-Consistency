package org.example.Server;

import java.util.function.LongSupplier;

/**
 * The replication resource (leader only): a rate, budgeted at the measured
 * maximum commit rate in entries per second. Every write costs one log entry,
 * charged at admission and never returned; reads cost zero, including
 * linearizable reads, since ReadIndex appends nothing. Refills continuously,
 * capped at one second's budget.
 */
public final class ReplicationRateBucket {

    private final double entriesPerSecond;
    private final LongSupplier clock;

    private double tokens;
    private long lastRefillNanos;

    public ReplicationRateBucket(double entriesPerSecond) {
        this(entriesPerSecond, System::nanoTime);
    }

    public ReplicationRateBucket(double entriesPerSecond, LongSupplier clock) {
        this.entriesPerSecond = entriesPerSecond;
        this.clock = clock;
        this.tokens = entriesPerSecond;
        this.lastRefillNanos = clock.getAsLong();
    }

    /** Charge one entry; false = bucket empty, the write must be rejected (hard backstop). */
    public synchronized boolean tryCharge() {
        long now = clock.getAsLong();
        tokens = Math.min(entriesPerSecond, tokens + (now - lastRefillNanos) / 1e9 * entriesPerSecond);
        lastRefillNanos = now;
        if (tokens >= 1.0) {
            tokens -= 1.0;
            return true;
        }
        return false;
    }

    public synchronized double tokensRemaining() {
        return tokens;
    }
}
