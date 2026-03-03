
package org.example.Utility;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
/**
 * EWMA-based backlog tracker.
 * Tracks smoothed backlog and detects increasing trend.
 */
public class BacklogTracker {

    private final double alpha;   // smoothing factor (0 < alpha <= 1)
    private final double delta;   // threshold for detecting “increasing” backlog
    private double ema = -1;      // smoothed backlog
    private double lastEma = -1;  // previous EMA for trend comparison
    private static final Path CSV_PATH = Paths.get("backlog_samples.csv");

    /**
     * @param alpha smoothing factor: smaller = smoother, larger = more responsive
     * @param delta threshold for detecting increasing backlog
     */
    public BacklogTracker(double alpha, double delta) {
        this.alpha = alpha;
        this.delta = delta;
    }

    /**
     * Add a new backlog sample.
     */
    public synchronized void addSample(int backlog) {
        if (ema < 0) {      // first sample
            ema = backlog;
            lastEma = backlog;
        } else {
            ema = alpha * backlog + (1 - alpha) * ema;
        }
        appendCsv(backlog, ema);
    }

    // Append a CSV line with timestamp (ms), backlog sample and current EMA.
    private void appendCsv(int backlog, double ema) {
        try {
            boolean exists = Files.exists(CSV_PATH);
            try (BufferedWriter bw = Files.newBufferedWriter(CSV_PATH,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                 PrintWriter pw = new PrintWriter(bw)) {
                if (!exists) {
                    pw.println("timestamp_ms,backlog,ema");
                }
                pw.printf("%d,%d,%.6f%n", System.currentTimeMillis(), backlog, ema);
            }
        } catch (IOException e) {
            // Non-fatal: log to stderr so we don't change method signatures
            System.err.println("Failed to write backlog sample to CSV: " + e.getMessage());
        }
    }

    /**
     * Check if backlog is increasing (trend detection).
     * Returns true if EMA increased more than delta since last check.
     */
    public synchronized boolean isIncreasing() {
        if (lastEma < 0) return false; // first sample

        boolean increasing = (ema - lastEma) > delta;
        lastEma = ema;
        return increasing;
    }

    /**
     * Get current smoothed backlog value.
     */
    public synchronized double getEma() {
        return ema;
    }
}