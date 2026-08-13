package org.example.Client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.example.Utility.RungScorer;
import org.example.raft.KvClientGrpc;
import org.example.raft.KvRequest;
import org.example.raft.KvResponse;
import org.example.raft.ReadLevel;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

/**
 * One application session against the cluster. Owns a stream per server, the
 * session anchors (uncommitted = highest log index among this session's
 * acknowledged writes; committed = highest among its majority-acknowledged
 * writes), a per-node RTT estimator fed by no-wait replies, and the client
 * side of the experiment arm ({@link ClientMode}):
 *
 * - chameleon: reads to the lowest-RTT node, the server scorer decides.
 * - chameleonPileus: reads routed by the Pileus selector, server decides.
 * - pileus: the selector picks (server, rung); the request carries the
 *   target (wantLinearizable / requestedWriteConcern) plus a wait bound of
 *   threshold minus RTT, the client's d_max analog.
 * - highestProfit / lowestProfit: static rung target, lowest-RTT routing.
 *
 * Every served response is graded client-side ({@link ClientGrader}) against
 * both returned views with client-observed latency; the verdict feeds the
 * ledger and the session-guarantee assertions. Writes and linearizable-only
 * targets go to the leader (learned from notLeader redirects).
 */
public final class KvSessionClient implements AutoCloseable {

    private final int applicationId;
    private final int numServers;
    private final int majority;
    private final ClientMode mode;
    private final int retryLimit;
    private final long lostTimeoutMs;
    private final boolean followerLinReads;

    // Full SLA tables (the application's own registration) and their floors.
    private final Map<Integer, List<RungScorer.Rung>> readSlas;
    private final Map<Integer, List<RungScorer.Rung>> writeSlas;
    private final Map<Integer, Integer> readFloors;
    private final Map<Integer, Integer> writeFloors;

    private final List<ManagedChannel> channels = new ArrayList<>();
    private final StreamObserver<KvRequest>[] streams;
    private final Object[] streamLocks;
    private final RttEstimator rttEstimator;
    private final PileusSelector selector; // null unless the mode routes with Pileus

    private final AtomicLong requestIdGen = new AtomicLong();
    private final AtomicInteger roundRobin = new AtomicInteger();
    private volatile int leaderHint = -1;

    // Session anchors and per-key write history for assertions.
    private final AtomicInteger uncommittedAnchor = new AtomicInteger(-1);
    private final AtomicInteger committedAnchor = new AtomicInteger(-1);
    private final ConcurrentHashMap<String, Integer> lastWriteIndexByKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> lastMajorityWriteIndexByKey = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Long, Pending> pending = new ConcurrentHashMap<>();
    private final ScheduledExecutorService lostSweeper;

    private final AtomicLong violations = new AtomicLong();
    private static final int VIOLATIONS_TO_PRINT = 10;

    private static final class Pending {
        final KvRequest request;
        final long firstSendNanos;
        final long firstSendMs;
        final int targetNode;
        final int attempt;
        // Send-time snapshots: assertion floors (null = the session had not
        // written the key), the client-side profit prediction, and the target
        // rung's threshold for recomputing the wait bound on retries.
        final Integer keyWriteSnapshot;
        final Integer keyMajorityWriteSnapshot;
        final double predictedProfit;
        final double targetThresholdMs;

        Pending(KvRequest request, long firstSendNanos, long firstSendMs, int targetNode, int attempt,
                Integer keyWriteSnapshot, Integer keyMajorityWriteSnapshot,
                double predictedProfit, double targetThresholdMs) {
            this.request = request;
            this.firstSendNanos = firstSendNanos;
            this.firstSendMs = firstSendMs;
            this.targetNode = targetNode;
            this.attempt = attempt;
            this.keyWriteSnapshot = keyWriteSnapshot;
            this.keyMajorityWriteSnapshot = keyMajorityWriteSnapshot;
            this.predictedProfit = predictedProfit;
            this.targetThresholdMs = targetThresholdMs;
        }
    }

    @SuppressWarnings("unchecked")
    public KvSessionClient(int applicationId, List<String> hosts, int basePort, ClientMode mode,
            int rttWindowSize, int retryLimit, long lostTimeoutMs,
            Map<Integer, List<RungScorer.Rung>> readSlas, Map<Integer, List<RungScorer.Rung>> writeSlas,
            double explorationFraction, boolean followerLinReads) {
        this.applicationId = applicationId;
        this.numServers = hosts.size();
        this.majority = (numServers / 2) + 1;
        this.mode = mode;
        this.retryLimit = retryLimit;
        this.lostTimeoutMs = lostTimeoutMs;
        this.followerLinReads = followerLinReads;
        this.readSlas = Map.copyOf(readSlas);
        this.writeSlas = Map.copyOf(writeSlas);
        this.readFloors = floorsOf(this.readSlas);
        this.writeFloors = floorsOf(this.writeSlas);
        this.streams = new StreamObserver[numServers];
        this.streamLocks = new Object[numServers];
        this.rttEstimator = new RttEstimator(numServers, rttWindowSize);
        this.selector = mode.pileusRouting()
                ? new PileusSelector(numServers, majority, rttWindowSize, followerLinReads,
                        explorationFraction, new Random())
                : null;

        for (int i = 0; i < numServers; i++) {
            final int nodeId = i;
            ManagedChannel channel = ManagedChannelBuilder.forAddress(hosts.get(i), basePort + i + 1)
                    .usePlaintext().build();
            channels.add(channel);
            streamLocks[i] = new Object();
            streams[i] = KvClientGrpc.newStub(channel).session(new StreamObserver<>() {
                @Override
                public void onNext(KvResponse response) {
                    handleResponse(nodeId, response);
                }

                @Override
                public void onError(Throwable t) {
                    // Stream died (node crashed or shut down). Outstanding
                    // requests on it will be scored as lost by the sweeper.
                }

                @Override
                public void onCompleted() {
                }
            });
        }

        this.lostSweeper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kv-client-lost-sweeper-app" + applicationId);
            t.setDaemon(true);
            return t;
        });
        this.lostSweeper.scheduleAtFixedRate(this::sweepLost, 1, 1, TimeUnit.SECONDS);
    }

    private static Map<Integer, Integer> floorsOf(Map<Integer, List<RungScorer.Rung>> slas) {
        Map<Integer, Integer> floors = new ConcurrentHashMap<>();
        slas.forEach((slaId, rungs) -> floors.put(slaId,
                rungs.stream().mapToInt(RungScorer.Rung::strength).min().orElseThrow()));
        return floors;
    }

    private List<RungScorer.Rung> slaOf(Map<Integer, List<RungScorer.Rung>> slas, int slaId) {
        List<RungScorer.Rung> rungs = slas.get(slaId);
        if (rungs == null) {
            throw new IllegalStateException("No SLA registered on the client for applicationId="
                    + applicationId + " slaId=" + slaId);
        }
        return rungs;
    }

    // ===== Sending =====

    public void sendRead(String key, int slaId) {
        // Snapshot the per-key assertion floors FIRST and fold them into the
        // session anchors (see stage 2: unordered ack updates would otherwise
        // let the wait anchor lag the floor the assertion checks against).
        Integer keyWriteSnapshot = lastWriteIndexByKey.get(key);
        Integer keyMajorityWriteSnapshot = lastMajorityWriteIndexByKey.get(key);
        int uncommitted = Math.max(uncommittedAnchor.get(), keyWriteSnapshot == null ? -1 : keyWriteSnapshot);
        int committed = Math.max(committedAnchor.get(),
                keyMajorityWriteSnapshot == null ? -1 : keyMajorityWriteSnapshot);

        List<RungScorer.Rung> sla = slaOf(readSlas, slaId);
        int targetNode;
        RungScorer.Rung targetRung = null;
        double predicted = 0;
        switch (mode) {
            case CHAMELEON -> targetNode = lowestRttNode();
            case CHAMELEON_PILEUS -> {
                PileusSelector.Choice choice = selector.chooseRead(sla, uncommitted, committed, leaderHint);
                targetNode = choice == null ? lowestRttNode() : choice.node();
            }
            case PILEUS -> {
                PileusSelector.Choice choice = selector.chooseRead(sla, uncommitted, committed, leaderHint);
                if (choice == null) {
                    targetNode = lowestRttNode();
                } else {
                    targetNode = choice.node();
                    targetRung = sla.get(choice.rungIndex());
                    predicted = choice.expectedProfit();
                }
            }
            default -> { // HIGHEST_PROFIT / LOWEST_PROFIT
                targetRung = sla.get(staticTargetIndex(sla, mode == ClientMode.HIGHEST_PROFIT));
                predicted = targetRung.profit();
                boolean linOnLeaderOnly = targetRung.strength() == ReadLevel.LINEARIZABLE.getNumber()
                        && !followerLinReads;
                targetNode = (linOnLeaderOnly && leaderHint >= 0) ? leaderHint : lowestRttNode();
            }
        }

        KvRequest.Builder request = KvRequest.newBuilder()
                .setRequestId(requestIdGen.incrementAndGet())
                .setApplicationId(applicationId)
                .setSlaId(slaId)
                .setIsRead(true)
                .setKey(key)
                .setCommittedSessionIndex(committed)
                .setUncommittedSessionIndex(uncommitted);
        if (targetRung != null) {
            request.setWantLinearizable(targetRung.strength() == ReadLevel.LINEARIZABLE.getNumber());
        }
        dispatch(request.build(), targetNode, 1, keyWriteSnapshot, keyMajorityWriteSnapshot,
                predicted, targetRung == null ? 0 : targetRung.thresholdMs());
    }

    public void sendWrite(String key, String value, int slaId) {
        List<RungScorer.Rung> sla = slaOf(writeSlas, slaId);
        int targetNode = pickWriteTarget();
        RungScorer.Rung targetRung = null;
        double predicted = 0;
        switch (mode) {
            case CHAMELEON, CHAMELEON_PILEUS -> {
                // Server decides the concern.
            }
            case PILEUS -> {
                PileusSelector.Choice choice = selector.chooseWrite(sla, targetNode);
                targetRung = sla.get(choice.rungIndex());
                predicted = choice.expectedProfit();
            }
            default -> {
                targetRung = sla.get(staticTargetIndex(sla, mode == ClientMode.HIGHEST_PROFIT));
                predicted = targetRung.profit();
            }
        }

        KvRequest.Builder request = KvRequest.newBuilder()
                .setRequestId(requestIdGen.incrementAndGet())
                .setApplicationId(applicationId)
                .setSlaId(slaId)
                .setIsRead(false)
                .setKey(key)
                .setValue(value)
                .setCommittedSessionIndex(committedAnchor.get())
                .setUncommittedSessionIndex(uncommittedAnchor.get());
        if (targetRung != null) {
            request.setRequestedWriteConcern(Math.min(Math.max(1, targetRung.strength()), majority));
        }
        dispatch(request.build(), targetNode, 1, null, null,
                predicted, targetRung == null ? 0 : targetRung.thresholdMs());
    }

    /** The max-profit or floor rung; profit ties go to the weakest requirement. */
    private static int staticTargetIndex(List<RungScorer.Rung> sla, boolean highest) {
        int best = 0;
        for (int i = 1; i < sla.size(); i++) {
            RungScorer.Rung rung = sla.get(i);
            RungScorer.Rung current = sla.get(best);
            boolean betterProfit = highest ? rung.profit() > current.profit() : rung.profit() < current.profit();
            if (betterProfit || (rung.profit() == current.profit() && rung.strength() < current.strength())) {
                best = i;
            }
        }
        return best;
    }

    /** Lowest estimated RTT; ties (including the all-cold start) break randomly. */
    private int lowestRttNode() {
        double best = Double.MAX_VALUE;
        int count = 0;
        int pick = 0;
        for (int i = 0; i < numServers; i++) {
            double estimate = rttEstimator.estimateMs(i);
            if (estimate < best) {
                best = estimate;
                count = 1;
                pick = i;
            } else if (estimate == best && ThreadLocalRandom.current().nextInt(++count) == 0) {
                pick = i;
            }
        }
        return pick;
    }

    private int pickWriteTarget() {
        int leader = leaderHint;
        return leader >= 0 ? leader : Math.floorMod(roundRobin.getAndIncrement(), numServers);
    }

    private void dispatch(KvRequest request, int targetNode, int attempt,
            Integer keyWriteSnapshot, Integer keyMajorityWriteSnapshot,
            double predictedProfit, double targetThresholdMs) {
        long nowNanos = System.nanoTime();
        long nowMs = System.currentTimeMillis();
        Pending previous = pending.get(request.getRequestId());
        Pending entry = (previous != null)
                // Retry: keep the original send instants and snapshots so
                // latency stays end to end and the prediction stays the first.
                ? new Pending(request, previous.firstSendNanos, previous.firstSendMs, targetNode, attempt,
                        previous.keyWriteSnapshot, previous.keyMajorityWriteSnapshot,
                        previous.predictedProfit, previous.targetThresholdMs)
                : new Pending(request, nowNanos, nowMs, targetNode, attempt,
                        keyWriteSnapshot, keyMajorityWriteSnapshot, predictedProfit, targetThresholdMs);
        pending.put(request.getRequestId(), entry);

        // The RTT estimate rides every request (the scorer's rho); in the
        // client-decided modes the wait bound is the d_max analog, the target
        // rung's threshold minus the network's share of it.
        double rtt = rttEstimator.estimateMs(targetNode);
        KvRequest.Builder withRtt = request.toBuilder().setRttEstimateMs(rtt);
        if (entry.targetThresholdMs > 0) {
            withRtt.setWaitBoundMs(Math.max(1.0, entry.targetThresholdMs - rtt));
        }
        try {
            synchronized (streamLocks[targetNode]) {
                streams[targetNode].onNext(withRtt.build());
            }
        } catch (RuntimeException e) {
            // Stream unusable (node down): the sweeper scores it as lost.
        }
    }

    // ===== Responses =====

    private void handleResponse(int nodeId, KvResponse response) {
        Pending entry = pending.get(response.getRequestId());
        if (entry == null) {
            return; // late duplicate after a retry resolved, or post-loss reply
        }
        KvRequest request = entry.request;
        String chosen = chosenLabel(request);

        if (response.getNotLeader()) {
            int hinted = response.getLeaderId();
            if (hinted >= 0 && hinted < numServers) {
                leaderHint = hinted;
            }
            ClientMetricsTracker.recordRedirect(nodeId, chosen);
            if (entry.attempt < retryLimit) {
                int target = (hinted >= 0 && hinted < numServers)
                        ? hinted
                        : Math.floorMod(roundRobin.getAndIncrement(), numServers);
                dispatch(request, target, entry.attempt + 1, null, null, 0, 0);
            } else {
                pending.remove(response.getRequestId());
                ClientMetricsTracker.recordFailure(nodeId, chosen);
            }
            return;
        }

        if (response.getRejected()) {
            // Admission shed this request; retrying would defeat the shedding,
            // but the selector must see the rejection or it would keep
            // targeting the rejecting node on cold-start optimism.
            pending.remove(response.getRequestId());
            if (selector != null) {
                if (request.getIsRead()) {
                    selector.observeReadRejected(nodeId, request.getWantLinearizable());
                } else {
                    selector.observeWriteRejected(request.getRequestedWriteConcern());
                }
            }
            ClientMetricsTracker.recordRejected(nodeId, chosen);
            return;
        }

        if (!response.getOk()) {
            if (entry.attempt < retryLimit) {
                dispatch(request, Math.floorMod(roundRobin.getAndIncrement(), numServers), entry.attempt + 1,
                        null, null, 0, 0);
            } else {
                pending.remove(response.getRequestId());
                ClientMetricsTracker.recordFailure(nodeId, chosen);
            }
            return;
        }

        pending.remove(response.getRequestId());
        double latencyMs = (System.nanoTime() - entry.firstSendNanos) / 1_000_000.0;
        rttEstimator.observe(nodeId, latencyMs, response.getServiceTimeMs(), response.getWaited());
        if (selector != null) {
            selector.observeIndices(nodeId, response.getLogIndex(), response.getCommitIndex());
        }

        ClientGrader.Verdict verdict;
        boolean upgraded;
        String executed;
        if (request.getIsRead()) {
            executed = "R:" + response.getDeliveredReadLevel().name();
            if (selector != null) {
                selector.observeRead(nodeId,
                        request.getWantLinearizable()
                                || response.getDeliveredReadLevel() == ReadLevel.LINEARIZABLE,
                        latencyMs);
            }
            verdict = ClientGrader.gradeRead(slaOf(readSlas, request.getSlaId()), response,
                    request.getUncommittedSessionIndex(), request.getCommittedSessionIndex(),
                    entry.keyWriteSnapshot, entry.keyMajorityWriteSnapshot, latencyMs);
            upgraded = verdict.gradedStrength() > readFloors.get(request.getSlaId());
            if (verdict.violation()) {
                long n = violations.incrementAndGet();
                if (n <= VIOLATIONS_TO_PRINT) {
                    System.err.printf(
                            "SESSION VIOLATION app=%d key=%s delivered=%s localIdx=%d committedIdx=%d expected>=%s/%s (node %d)%n",
                            applicationId, request.getKey(), response.getDeliveredReadLevel(),
                            response.getLocalValueIndex(), response.getCommittedValueIndex(),
                            entry.keyWriteSnapshot, entry.keyMajorityWriteSnapshot, nodeId);
                }
            }
        } else {
            int index = response.getValueIndex();
            executed = "W:" + response.getDeliveredWriteConcern();
            if (selector != null) {
                selector.observeWrite(request.getRequestedWriteConcern() > 0
                        ? request.getRequestedWriteConcern()
                        : response.getDeliveredWriteConcern(), latencyMs);
            }
            verdict = ClientGrader.gradeWrite(slaOf(writeSlas, request.getSlaId()), response, latencyMs);
            upgraded = response.getDeliveredWriteConcern() > writeFloors.get(request.getSlaId());
            lastWriteIndexByKey.merge(request.getKey(), index, Math::max);
            uncommittedAnchor.accumulateAndGet(index, Math::max);
            if (response.getDeliveredWriteConcern() >= majority && !response.getTimedOutAndFellBack()) {
                lastMajorityWriteIndexByKey.merge(request.getKey(), index, Math::max);
                committedAnchor.accumulateAndGet(index, Math::max);
            }
        }

        double predicted = mode.chameleonDecision() ? response.getPredictedProfit() : entry.predictedProfit;
        ClientMetricsTracker.recordResponse(nodeId, chosen, executed, latencyMs,
                response.getTimedOutAndFellBack(), verdict.violation(),
                predicted, verdict.realizedProfit(), verdict.satisfiedRung(), upgraded, response.getWaited());
    }

    private static String chosenLabel(KvRequest request) {
        return (request.getIsRead() ? "R:" : "W:") + "A" + request.getApplicationId() + "S" + request.getSlaId();
    }

    private void sweepLost() {
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<Long, Pending>> it = pending.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Pending> e = it.next();
            if (now - e.getValue().firstSendMs >= lostTimeoutMs) {
                it.remove();
                ClientMetricsTracker.recordLost(e.getValue().targetNode, chosenLabel(e.getValue().request));
            }
        }
    }

    public long sessionViolations() {
        return violations.get();
    }

    public int pendingCount() {
        return pending.size();
    }

    @Override
    public void close() {
        lostSweeper.shutdownNow();
        for (int i = 0; i < numServers; i++) {
            try {
                synchronized (streamLocks[i]) {
                    streams[i].onCompleted();
                }
            } catch (RuntimeException ignored) {
                // Stream already dead.
            }
        }
        for (ManagedChannel channel : channels) {
            channel.shutdownNow();
        }
    }
}
