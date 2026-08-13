package org.example.Client;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.DoubleAdder;

/**
 * Client-side ledger: every cross-arm comparison metric lives here, measured
 * at the client from the request/response stream. Rows are cumulative per
 * (node, chosen level, executed level); consumers diff consecutive rows for
 * per-interval rates, which keeps the file lossless if a flush is skipped.
 *
 * Chosen is what the workload asked for; executed is what the server
 * delivered (fallbacks make them differ). Redirect resends, hard failures,
 * lost requests (no response), and session-guarantee violations are counted
 * per cell.
 *
 * Stage 5 adds the four instrumentation streams: predicted vs realized profit
 * (sums per cell; a misprediction is an accounting error, not just a missed
 * deadline), free vs waiting upgrades above the SLA floor, client-observed
 * latency tails per cell (geometric histogram, P50/P95/P99, not averages),
 * and the satisfied rung per request (counts per rung index, the direct
 * head-to-head metric against Pileus).
 */
public final class ClientMetricsTracker {

    private ClientMetricsTracker() {
    }

    private static final String CSV_PATH = "client_metrics_global.csv";
    private static final long FLUSH_INTERVAL_MS = 1000;

    // Latency histogram: the same geometric bucketing as the server's
    // service-time histograms (64 buckets from 0.5 ms, ratio 1.15) plus one
    // overflow bucket.
    private static final int LATENCY_BUCKETS = 64;
    private static final double LATENCY_BASE_MS = 0.5;
    private static final double LATENCY_RATIO = 1.15;
    private static final double LOG_RATIO = Math.log(LATENCY_RATIO);

    /** SLAs may register at most this many rungs (fixed satisfied-rung columns). */
    public static final int MAX_RUNGS = 4;

    private record Key(int nodeId, String chosen, String executed) {
    }

    private static final class Cell {
        final AtomicLong count = new AtomicLong();
        final AtomicLong fallbacks = new AtomicLong();
        final AtomicLong redirects = new AtomicLong();
        final AtomicLong failures = new AtomicLong();
        final AtomicLong rejected = new AtomicLong();
        final AtomicLong lost = new AtomicLong();
        final AtomicLong violations = new AtomicLong();
        final DoubleAdder latencySumMs = new DoubleAdder();
        final DoubleAdder predictedProfitSum = new DoubleAdder();
        final DoubleAdder realizedProfitSum = new DoubleAdder();
        final AtomicLong upgradesFree = new AtomicLong();
        final AtomicLong upgradesWaiting = new AtomicLong();
        // Index MAX_RUNGS counts responses where no rung was satisfied.
        final AtomicLongArray satisfiedRung = new AtomicLongArray(MAX_RUNGS + 1);
        final AtomicLongArray latencyBuckets = new AtomicLongArray(LATENCY_BUCKETS + 1);
    }

    private static final ConcurrentHashMap<Key, Cell> cells = new ConcurrentHashMap<>();
    private static final AtomicLong lastFlushMs = new AtomicLong(0);

    private static Cell cell(int nodeId, String chosen, String executed) {
        return cells.computeIfAbsent(new Key(nodeId, chosen, executed), k -> new Cell());
    }

    private static int latencyBucketOf(double latencyMs) {
        if (latencyMs <= LATENCY_BASE_MS) {
            return 0;
        }
        int index = (int) Math.floor(Math.log(latencyMs / LATENCY_BASE_MS) / LOG_RATIO);
        return Math.min(index, LATENCY_BUCKETS);
    }

    /** Upper edge of a latency bucket, the value quantiles report. */
    private static double latencyBucketUpperMs(int bucket) {
        return LATENCY_BASE_MS * Math.pow(LATENCY_RATIO, bucket + 1.0);
    }

    /**
     * One served response. {@code satisfiedRung} is the server-graded rung
     * index (-1 = none met); {@code upgraded} means the graded delivery was
     * above the SLA's floor, split into free vs waiting by {@code waited}.
     */
    public static void recordResponse(int nodeId, String chosen, String executed, double latencyMs,
            boolean fellBack, boolean violation, double predictedProfit, double realizedProfit,
            int satisfiedRung, boolean upgraded, boolean waited) {
        Cell c = cell(nodeId, chosen, executed);
        c.count.incrementAndGet();
        c.latencySumMs.add(latencyMs);
        c.latencyBuckets.incrementAndGet(latencyBucketOf(latencyMs));
        c.predictedProfitSum.add(predictedProfit);
        c.realizedProfitSum.add(realizedProfit);
        if (satisfiedRung >= MAX_RUNGS) {
            throw new IllegalArgumentException("satisfiedRung " + satisfiedRung + " exceeds MAX_RUNGS " + MAX_RUNGS);
        }
        c.satisfiedRung.incrementAndGet(satisfiedRung < 0 ? MAX_RUNGS : satisfiedRung);
        if (upgraded) {
            (waited ? c.upgradesWaiting : c.upgradesFree).incrementAndGet();
        }
        if (fellBack) {
            c.fallbacks.incrementAndGet();
        }
        if (violation) {
            c.violations.incrementAndGet();
        }
        maybeFlush();
    }

    public static void recordRedirect(int nodeId, String chosen) {
        cell(nodeId, chosen, "-").redirects.incrementAndGet();
        maybeFlush();
    }

    public static void recordFailure(int nodeId, String chosen) {
        cell(nodeId, chosen, "-").failures.incrementAndGet();
        maybeFlush();
    }

    /** Admission control shed the request; not retried by design. */
    public static void recordRejected(int nodeId, String chosen) {
        cell(nodeId, chosen, "-").rejected.incrementAndGet();
        maybeFlush();
    }

    public static long totalRejected() {
        long total = 0;
        for (Cell c : cells.values()) {
            total += c.rejected.get();
        }
        return total;
    }

    public static void recordLost(int nodeId, String chosen) {
        cell(nodeId, chosen, "-").lost.incrementAndGet();
        maybeFlush();
    }

    public static long totalViolations() {
        long total = 0;
        for (Cell c : cells.values()) {
            total += c.violations.get();
        }
        return total;
    }

    public static long totalResponses() {
        long total = 0;
        for (Cell c : cells.values()) {
            total += c.count.get();
        }
        return total;
    }

    public static long totalLost() {
        long total = 0;
        for (Cell c : cells.values()) {
            total += c.lost.get();
        }
        return total;
    }

    public static double totalPredictedProfit() {
        double total = 0;
        for (Cell c : cells.values()) {
            total += c.predictedProfitSum.sum();
        }
        return total;
    }

    public static double totalRealizedProfit() {
        double total = 0;
        for (Cell c : cells.values()) {
            total += c.realizedProfitSum.sum();
        }
        return total;
    }

    private static double quantileMs(AtomicLongArray buckets, double quantile) {
        long total = 0;
        for (int i = 0; i <= LATENCY_BUCKETS; i++) {
            total += buckets.get(i);
        }
        if (total == 0) {
            return 0.0;
        }
        long target = (long) Math.ceil(quantile * total);
        long cumulative = 0;
        for (int i = 0; i <= LATENCY_BUCKETS; i++) {
            cumulative += buckets.get(i);
            if (cumulative >= target) {
                return latencyBucketUpperMs(i);
            }
        }
        return latencyBucketUpperMs(LATENCY_BUCKETS);
    }

    private static void maybeFlush() {
        long now = System.currentTimeMillis();
        long last = lastFlushMs.get();
        if (now - last >= FLUSH_INTERVAL_MS && lastFlushMs.compareAndSet(last, now)) {
            flush(now);
        }
    }

    /** Force a flush (end of run). */
    public static void flushNow() {
        flush(System.currentTimeMillis());
    }

    private static synchronized void flush(long now) {
        File file = new File(CSV_PATH);
        boolean writeHeader = !file.exists() || file.length() == 0;
        try (FileWriter fw = new FileWriter(file, true); PrintWriter out = new PrintWriter(fw)) {
            if (writeHeader) {
                out.println("Timestamp,NodeId,ChosenLevel,ExecutedLevel,CountTotal,AvgLatencyMs,"
                        + "P50Ms,P95Ms,P99Ms,PredictedProfitSum,RealizedProfitSum,"
                        + "UpgradesFreeTotal,UpgradesWaitingTotal,"
                        + "SatisfiedRung0,SatisfiedRung1,SatisfiedRung2,SatisfiedRung3,SatisfiedNone,"
                        + "FallbacksTotal,RedirectsTotal,FailuresTotal,RejectedTotal,LostTotal,SessionViolationsTotal");
            }
            for (Map.Entry<Key, Cell> e : cells.entrySet()) {
                Key k = e.getKey();
                Cell c = e.getValue();
                long count = c.count.get();
                double avg = count == 0 ? 0.0 : c.latencySumMs.sum() / count;
                out.printf("%d,%d,%s,%s,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
                        now, k.nodeId(), k.chosen(), k.executed(), count, avg,
                        quantileMs(c.latencyBuckets, 0.50), quantileMs(c.latencyBuckets, 0.95),
                        quantileMs(c.latencyBuckets, 0.99),
                        c.predictedProfitSum.sum(), c.realizedProfitSum.sum(),
                        c.upgradesFree.get(), c.upgradesWaiting.get(),
                        c.satisfiedRung.get(0), c.satisfiedRung.get(1), c.satisfiedRung.get(2),
                        c.satisfiedRung.get(3), c.satisfiedRung.get(MAX_RUNGS),
                        c.fallbacks.get(), c.redirects.get(), c.failures.get(), c.rejected.get(), c.lost.get(),
                        c.violations.get());
            }
        } catch (IOException e) {
            System.err.println("Failed to write " + CSV_PATH + ": " + e.getMessage());
        }
    }
}
