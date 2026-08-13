package org.example.Client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.example.raft.KvClientGrpc;
import org.example.raft.KvRequest;
import org.example.raft.KvResponse;
import org.example.raft.ReadLevel;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

/**
 * One application session against the cluster (Chameleon stage 2).
 *
 * Owns a stream per server, the session anchors (uncommitted = highest log
 * index among this session's acknowledged writes; committed = highest index
 * among its majority-acknowledged writes), a per-node RTT estimate from
 * no-wait replies (end-to-end latency minus the server-reported service
 * time, median of a sliding window), and the session-guarantee assertions:
 * a causal-local read must return a value at least as new as this session's
 * last acknowledged write to that key, causal-majority and linearizable
 * reads at least as new as its last majority-acknowledged write.
 *
 * Writes and linearizable reads target the leader (learned from notLeader
 * redirects); everything else round-robins. Redirected or failed requests
 * are resent up to retryLimit times; requests with no response within
 * lostTimeoutMs are scored as lost.
 */
public final class KvSessionClient implements AutoCloseable {

    private final int applicationId;
    private final int numServers;
    private final int retryLimit;
    private final long lostTimeoutMs;
    private final int majority;

    private final List<ManagedChannel> channels = new ArrayList<>();
    private final StreamObserver<KvRequest>[] streams;
    private final Object[] streamLocks;
    private final SlidingWindow[] rttWindows;

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
        // Assertion snapshots taken at send time (null = this session had not
        // written the key yet, so there is nothing to assert).
        final Integer keyWriteSnapshot;
        final Integer keyMajorityWriteSnapshot;

        Pending(KvRequest request, long firstSendNanos, long firstSendMs, int targetNode, int attempt,
                Integer keyWriteSnapshot, Integer keyMajorityWriteSnapshot) {
            this.request = request;
            this.firstSendNanos = firstSendNanos;
            this.firstSendMs = firstSendMs;
            this.targetNode = targetNode;
            this.attempt = attempt;
            this.keyWriteSnapshot = keyWriteSnapshot;
            this.keyMajorityWriteSnapshot = keyMajorityWriteSnapshot;
        }
    }

    @SuppressWarnings("unchecked")
    public KvSessionClient(int applicationId, List<String> hosts, int basePort, int rttWindowSize,
            int retryLimit, long lostTimeoutMs) {
        this.applicationId = applicationId;
        this.numServers = hosts.size();
        this.retryLimit = retryLimit;
        this.lostTimeoutMs = lostTimeoutMs;
        this.majority = (numServers / 2) + 1;
        this.streams = new StreamObserver[numServers];
        this.streamLocks = new Object[numServers];
        this.rttWindows = new SlidingWindow[numServers];

        for (int i = 0; i < numServers; i++) {
            final int nodeId = i;
            ManagedChannel channel = ManagedChannelBuilder.forAddress(hosts.get(i), basePort + i + 1)
                    .usePlaintext().build();
            channels.add(channel);
            streamLocks[i] = new Object();
            rttWindows[i] = new SlidingWindow(rttWindowSize);
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

    // ===== Sending =====

    public void sendRead(String key, int slaId) {
        // Snapshot the per-key assertion floors FIRST and fold them into the
        // session anchors. The anchors and the per-key map are updated by ack
        // threads in no particular order relative to this thread, so taking
        // the anchor alone could yield a wait anchor below the floor the
        // assertion later checks against; max() restores the invariant that
        // the request always waits at least as far as the assertion expects.
        Integer keyWriteSnapshot = lastWriteIndexByKey.get(key);
        Integer keyMajorityWriteSnapshot = lastMajorityWriteIndexByKey.get(key);
        int uncommitted = Math.max(uncommittedAnchor.get(), keyWriteSnapshot == null ? -1 : keyWriteSnapshot);
        int committed = Math.max(committedAnchor.get(),
                keyMajorityWriteSnapshot == null ? -1 : keyMajorityWriteSnapshot);
        KvRequest request = KvRequest.newBuilder()
                .setRequestId(requestIdGen.incrementAndGet())
                .setApplicationId(applicationId)
                .setSlaId(slaId)
                .setIsRead(true)
                .setKey(key)
                .setCommittedSessionIndex(committed)
                .setUncommittedSessionIndex(uncommitted)
                .build();
        dispatch(request, pickTarget(request), 1, keyWriteSnapshot, keyMajorityWriteSnapshot);
    }

    public void sendWrite(String key, String value, int slaId) {
        KvRequest request = KvRequest.newBuilder()
                .setRequestId(requestIdGen.incrementAndGet())
                .setApplicationId(applicationId)
                .setSlaId(slaId)
                .setIsRead(false)
                .setKey(key)
                .setValue(value)
                .setCommittedSessionIndex(committedAnchor.get())
                .setUncommittedSessionIndex(uncommittedAnchor.get())
                .build();
        dispatch(request, pickTarget(request), 1, null, null);
    }

    private int pickTarget(KvRequest request) {
        // Writes are leader-only; reads round-robin (the server redirects
        // when an SLA is unservable on a follower).
        int leader = leaderHint;
        if (!request.getIsRead() && leader >= 0) {
            return leader;
        }
        return Math.floorMod(roundRobin.getAndIncrement(), numServers);
    }

    private void dispatch(KvRequest request, int targetNode, int attempt,
            Integer keyWriteSnapshot, Integer keyMajorityWriteSnapshot) {
        long nowNanos = System.nanoTime();
        long nowMs = System.currentTimeMillis();
        Pending previous = pending.get(request.getRequestId());
        Pending entry = (previous != null)
                // Retry: keep the original send instants so latency stays end to end.
                ? new Pending(request, previous.firstSendNanos, previous.firstSendMs, targetNode, attempt,
                        previous.keyWriteSnapshot, previous.keyMajorityWriteSnapshot)
                : new Pending(request, nowNanos, nowMs, targetNode, attempt,
                        keyWriteSnapshot, keyMajorityWriteSnapshot);
        pending.put(request.getRequestId(), entry);

        // RTT estimate rides the request so the server can subtract it from
        // SLA thresholds (used by the scorer from stage 4 on).
        KvRequest withRtt = request.toBuilder()
                .setRttEstimateMs(rttWindows[targetNode].medianOr(0.0))
                .build();
        try {
            synchronized (streamLocks[targetNode]) {
                streams[targetNode].onNext(withRtt);
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
                // Retries reuse the snapshots stored in the pending entry.
                dispatch(request, target, entry.attempt + 1, null, null);
            } else {
                pending.remove(response.getRequestId());
                ClientMetricsTracker.recordFailure(nodeId, chosen);
            }
            return;
        }

        if (response.getRejected()) {
            // Admission shed this request; retrying would defeat the shedding.
            pending.remove(response.getRequestId());
            ClientMetricsTracker.recordRejected(nodeId, chosen);
            return;
        }

        if (!response.getOk()) {
            if (entry.attempt < retryLimit) {
                dispatch(request, Math.floorMod(roundRobin.getAndIncrement(), numServers), entry.attempt + 1,
                        null, null);
            } else {
                pending.remove(response.getRequestId());
                ClientMetricsTracker.recordFailure(nodeId, chosen);
            }
            return;
        }

        pending.remove(response.getRequestId());
        double latencyMs = (System.nanoTime() - entry.firstSendNanos) / 1_000_000.0;

        // RTT sample: only replies that involved no server-side waiting, so
        // the estimate does not absorb queueing or index waits.
        if (!response.getWaited()) {
            double rtt = latencyMs - response.getServiceTimeMs();
            if (rtt >= 0) {
                rttWindows[nodeId].add(rtt);
            }
        }

        boolean violation = false;
        String executed;
        if (request.getIsRead()) {
            executed = "R:" + response.getDeliveredReadLevel().name();
            violation = checkSessionGuarantee(entry, response);
            if (violation) {
                long n = violations.incrementAndGet();
                if (n <= VIOLATIONS_TO_PRINT) {
                    System.err.printf(
                            "SESSION VIOLATION app=%d key=%s level=%s valueIndex=%d expected>=%s (node %d)%n",
                            applicationId, request.getKey(), response.getDeliveredReadLevel(),
                            response.getValueIndex(),
                            response.getDeliveredReadLevel() == ReadLevel.CAUSAL_LOCAL
                                    ? entry.keyWriteSnapshot
                                    : entry.keyMajorityWriteSnapshot,
                            nodeId);
                }
            }
        } else {
            int index = response.getValueIndex();
            executed = "W:" + response.getDeliveredWriteConcern();
            lastWriteIndexByKey.merge(request.getKey(), index, Math::max);
            uncommittedAnchor.accumulateAndGet(index, Math::max);
            if (response.getDeliveredWriteConcern() >= majority && !response.getTimedOutAndFellBack()) {
                lastMajorityWriteIndexByKey.merge(request.getKey(), index, Math::max);
                committedAnchor.accumulateAndGet(index, Math::max);
            }
        }

        ClientMetricsTracker.recordResponse(nodeId, chosen, executed, latencyMs,
                response.getTimedOutAndFellBack(), violation);
    }

    /**
     * Read-your-writes at the causal levels: the value's index must cover this
     * session's last acknowledged write to the key (snapshotted at send time).
     * Fallback responses carry a weaker delivered level and are exempt by
     * construction (the check keys on what was delivered, not requested).
     */
    private boolean checkSessionGuarantee(Pending entry, KvResponse response) {
        switch (response.getDeliveredReadLevel()) {
            case CAUSAL_LOCAL:
                return entry.keyWriteSnapshot != null && response.getValueIndex() < entry.keyWriteSnapshot;
            case CAUSAL_MAJORITY:
            case LINEARIZABLE:
                return entry.keyMajorityWriteSnapshot != null
                        && response.getValueIndex() < entry.keyMajorityWriteSnapshot;
            default:
                return false;
        }
    }

    private static String chosenLabel(KvRequest request) {
        return (request.getIsRead() ? "R:" : "W:") + "A" + request.getApplicationId() + "S" + request.getSlaId();
    }

    private void sweepLost(){
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
