package org.example.Client;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.example.raft.AckMessage;
import org.example.raft.ReadConcern;
import org.example.raft.ReadLevel;
import org.example.Utility.ExperimentConfig;
import org.example.Utility.TransactionOption;

/**
 * Client-side ledger for protocol comparison. Tracks every injected request
 * from send to resolution and scores it from the client's perspective:
 *
 *   hit      - ACK received within the app's latency deadline (profit earned)
 *   miss     - ACK received but too late (profit 0)
 *   rejected - refused at the server's admission gate (known synchronously
 *              from the accepted-prefix return value; profit 0)
 *   lost     - no ACK within lostTimeoutMs (profit 0)
 *
 * Latency, throughput (completions/s), and profit are all computed here, from
 * client-observed data only. Profit is priced on the EXECUTED consistency
 * level reported in the ACK (executedLevelIncluded), so server-side upgrades
 * are credited truthfully. Aggregates are flushed to
 * client_metrics_global.csv once per second, one row per (node, level).
 *
 * Mode-independent by design: this class observes and never influences
 * routing or level selection.
 */
public final class ClientMetricsTracker {

    private ClientMetricsTracker() {
    }

    private static final String CSV_PATH = "client_metrics_global.csv";
    private static final long FLUSH_INTERVAL_MS = 1000;

    // appId -> latency deadline in ms (from config)
    private static volatile Map<Integer, Integer> deadlinesMsByApp = Map.of();
    private static volatile long lostTimeoutMs = 10000;

    private static final class Pending {
        final long sendTimeMs;
        final int nodeId;
        final int appId;
        final boolean isRead;
        final String chosenLevel;

        Pending(long sendTimeMs, int nodeId, int appId, boolean isRead, String chosenLevel) {
            this.sendTimeMs = sendTimeMs;
            this.nodeId = nodeId;
            this.appId = appId;
            this.isRead = isRead;
            this.chosenLevel = chosenLevel;
        }
    }

    private static final class Cell {
        long hits;
        long misses;
        long rejected;
        long lost;
        double profit;
        final List<Long> latenciesMs = new ArrayList<>();
    }

    private static final ConcurrentHashMap<String, Pending> pending = new ConcurrentHashMap<>();
    private static final AtomicReference<ConcurrentHashMap<String, Cell>> interval =
            new AtomicReference<>(new ConcurrentHashMap<>());
    private static volatile ScheduledExecutorService flusher;

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    public static synchronized void configure(ExperimentConfig config) {
        Map<Integer, Integer> deadlines = new ConcurrentHashMap<>();
        for (Map.Entry<String, Integer> e : config.clientMetrics.deadlinesMsByApp.entrySet()) {
            deadlines.put(Integer.parseInt(e.getKey()), e.getValue());
        }
        deadlinesMsByApp = deadlines;
        lostTimeoutMs = config.clientMetrics.lostTimeoutMs;

        if (flusher == null) {
            flusher = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "client-metrics-flusher");
                t.setDaemon(true);
                return t;
            });
            flusher.scheduleAtFixedRate(ClientMetricsTracker::flushAndSweep,
                    FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        }
    }

    // ------------------------------------------------------------------
    // Producers (injector side)
    // ------------------------------------------------------------------

    /** Record a transaction accepted into a server's queue. */
    public static void register(TransactionOption tx, int nodeId) {
        long sendTime = tx.clientMessage.getT().getTransactionSendTimeInMs();
        if (sendTime <= 0) {
            sendTime = System.currentTimeMillis();
        }
        pending.put(tx.id, new Pending(sendTime, nodeId, tx.applicationId, tx.isReadOnly, chosenLevelLabel(tx)));
    }

    /** Record a transaction refused at the admission gate (never enqueued). */
    public static void recordRejected(TransactionOption tx, int nodeId) {
        Cell cell = cell(nodeId, chosenLevelLabel(tx), NOT_EXECUTED);
        synchronized (cell) {
            cell.rejected++;
        }
    }

    // ------------------------------------------------------------------
    // Consumer (ACK side)
    // ------------------------------------------------------------------

    /** Resolve an ACK against the pending ledger. Idempotent per transaction id. */
    public static void onAck(AckMessage ack) {
        String id = ack.getId();
        if (id == null || id.isEmpty()) {
            id = ack.getT().getId();
        }
        if (id == null || id.isEmpty()) {
            return;
        }
        Pending p = pending.remove(id);
        if (p == null) {
            return; // duplicate ACK, or a request we never registered
        }

        if (ack.getFailure()) {
            Cell cell = cell(p.nodeId, p.chosenLevel, NOT_EXECUTED);
            synchronized (cell) {
                cell.lost++;
            }
            return;
        }

        long latency = System.currentTimeMillis() - p.sendTimeMs;
        String executedLevel = executedLevelLabel(ack, p);
        Integer deadline = deadlinesMsByApp.get(p.appId);
        if (deadline == null) {
            throw new IllegalStateException("No latency deadline configured for applicationId " + p.appId
                    + " (clientMetrics.deadlinesMsByApp)");
        }
        boolean hit = latency <= deadline;

        Cell cell = cell(p.nodeId, p.chosenLevel, executedLevel);
        synchronized (cell) {
            if (hit) {
                cell.hits++;
                cell.profit += profitOf(executedLevel, p.appId);
            } else {
                cell.misses++;
            }
            cell.latenciesMs.add(latency);
        }

        // Feed the routing policy's estimator (no-op unless LATENCY_AWARE).
        LatencyAwareRouter.onSample(p.nodeId, p.isRead, p.chosenLevel, executedLevel, latency);
    }

    // ------------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------------

    /** ExecutedLevel value for requests that never executed (rejected/lost). */
    private static final String NOT_EXECUTED = "-";

    private static Cell cell(int nodeId, String chosenLevel, String executedLevel) {
        return interval.get().computeIfAbsent(nodeId + "," + chosenLevel + "," + executedLevel, k -> new Cell());
    }

    static String chosenLevelLabel(TransactionOption tx) {
        if (!tx.isReadOnly) {
            return "W:" + tx.clientMessage.getT().getWriteConcern();
        }
        return readLevelLabel(tx.readConcern, tx.readLevel);
    }

    private static String executedLevelLabel(AckMessage ack, Pending p) {
        if (!ack.getExecutedLevelIncluded()) {
            return p.chosenLevel; // server did not report; fall back to what was sent
        }
        if (p.isRead) {
            return readLevelLabel(ack.getExecutedReadConcern(), ack.getExecutedReadLevel());
        }
        return "W:" + ack.getExecutedWriteConcern();
    }

    private static String readLevelLabel(ReadConcern rc, ReadLevel rl) {
        if (rc == ReadConcern.LINEARIZABLE) {
            return "R:LINEARIZABLE";
        }
        if (rc == ReadConcern.EVENTUAL) {
            return "R:EVENTUAL";
        }
        return (rl == ReadLevel.MAJORITY) ? "R:CAUSAL_MAJORITY" : "R:CAUSAL_LOCAL";
    }

    /**
     * Profit schedule, mirroring the injection-time model where base and all
     * step profits equal the applicationId k:
     *   reads:  EVENTUAL=k, CAUSAL_LOCAL=2k, CAUSAL_MAJORITY=3k, LINEARIZABLE=4k
     *   writes: W:c = c*k
     */
    private static double profitOf(String level, int appId) {
        int multiplier;
        switch (level) {
            case "R:EVENTUAL" -> multiplier = 1;
            case "R:CAUSAL_LOCAL" -> multiplier = 2;
            case "R:CAUSAL_MAJORITY" -> multiplier = 3;
            case "R:LINEARIZABLE" -> multiplier = 4;
            default -> {
                if (!level.startsWith("W:")) {
                    throw new IllegalStateException("Unknown consistency level label: " + level);
                }
                multiplier = Integer.parseInt(level.substring(2));
            }
        }
        return (double) multiplier * appId;
    }

    private static void flushAndSweep() {
        try {
            long now = System.currentTimeMillis();

            // Sweep pending entries that exceeded the lost timeout.
            Iterator<Map.Entry<String, Pending>> it = pending.entrySet().iterator();
            while (it.hasNext()) {
                Pending p = it.next().getValue();
                if (now - p.sendTimeMs >= lostTimeoutMs) {
                    it.remove();
                    Cell cell = cell(p.nodeId, p.chosenLevel, NOT_EXECUTED);
                    synchronized (cell) {
                        cell.lost++;
                    }
                }
            }

            // Swap the interval map and write one row per (node, level).
            ConcurrentHashMap<String, Cell> cells = interval.getAndSet(new ConcurrentHashMap<>());
            if (cells.isEmpty()) {
                return;
            }

            File file = new File(CSV_PATH);
            boolean writeHeader = !file.exists() || file.length() == 0;
            try (FileWriter fw = new FileWriter(CSV_PATH, true); PrintWriter out = new PrintWriter(fw)) {
                if (writeHeader) {
                    out.println("Timestamp,Node,ChosenLevel,ExecutedLevel,Hits,Misses,Rejected,Lost,Profit,MeanLatencyMs,P99LatencyMs");
                }
                for (Map.Entry<String, Cell> e : cells.entrySet()) {
                    Cell c = e.getValue();
                    double mean = 0;
                    long p99 = 0;
                    synchronized (c) {
                        if (!c.latenciesMs.isEmpty()) {
                            Collections.sort(c.latenciesMs);
                            long sum = 0;
                            for (long l : c.latenciesMs) {
                                sum += l;
                            }
                            mean = (double) sum / c.latenciesMs.size();
                            p99 = c.latenciesMs.get((int) Math.ceil(c.latenciesMs.size() * 0.99) - 1);
                        }
                        out.printf("%d,%s,%d,%d,%d,%d,%.2f,%.2f,%d%n",
                                now, e.getKey(), c.hits, c.misses, c.rejected, c.lost, c.profit, mean, p99);
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Failed to write " + CSV_PATH + ": " + e.getMessage());
        } catch (Exception e) {
            System.err.println("client-metrics flush error: " + e.getMessage());
        }
    }
}
