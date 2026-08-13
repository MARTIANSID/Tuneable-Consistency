package org.example.Client;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
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
 */
public final class ClientMetricsTracker {

    private ClientMetricsTracker() {
    }

    private static final String CSV_PATH = "client_metrics_global.csv";
    private static final long FLUSH_INTERVAL_MS = 1000;

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
    }

    private static final ConcurrentHashMap<Key, Cell> cells = new ConcurrentHashMap<>();
    private static final AtomicLong lastFlushMs = new AtomicLong(0);

    private static Cell cell(int nodeId, String chosen, String executed) {
        return cells.computeIfAbsent(new Key(nodeId, chosen, executed), k -> new Cell());
    }

    public static void recordResponse(int nodeId, String chosen, String executed, double latencyMs,
            boolean fellBack, boolean violation) {
        Cell c = cell(nodeId, chosen, executed);
        c.count.incrementAndGet();
        c.latencySumMs.add(latencyMs);
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
                out.println("Timestamp,NodeId,ChosenLevel,ExecutedLevel,CountTotal,AvgLatencyMs,FallbacksTotal,RedirectsTotal,FailuresTotal,RejectedTotal,LostTotal,SessionViolationsTotal");
            }
            for (Map.Entry<Key, Cell> e : cells.entrySet()) {
                Key k = e.getKey();
                Cell c = e.getValue();
                long count = c.count.get();
                double avg = count == 0 ? 0.0 : c.latencySumMs.sum() / count;
                out.printf("%d,%d,%s,%s,%d,%.3f,%d,%d,%d,%d,%d,%d%n",
                        now, k.nodeId(), k.chosen(), k.executed(), count, avg,
                        c.fallbacks.get(), c.redirects.get(), c.failures.get(), c.rejected.get(), c.lost.get(),
                        c.violations.get());
            }
        } catch (IOException e) {
            System.err.println("Failed to write " + CSV_PATH + ": " + e.getMessage());
        }
    }
}
