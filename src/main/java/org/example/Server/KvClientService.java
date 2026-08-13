package org.example.Server;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.example.raft.KvClientGrpc;
import org.example.raft.KvRequest;
import org.example.raft.KvResponse;
import org.example.raft.ReadLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;

/**
 * The client-facing per-request path (Chameleon stage 2 + 3). Every request
 * is timestamped the moment it comes off the stream (t_recv, monotonic
 * clock), takes an occupancy slot until its reply, is executed at its forced
 * level, and files its measured service time into the histogram cell of the
 * level it actually executed under and the gap bucket it ran under.
 *
 * Levels are pure mechanics here: eventual levels read a view immediately,
 * causal levels wait for the relevant index to reach the client's session
 * anchor, linearizable runs a ReadIndex confirmation on the leader, and write
 * concerns wait for replication or commit. Every wait is bounded by
 * maxWaitMs; on expiry the request falls back to the strongest level that
 * needs no waiting and says so in the response. An abandoned wait files its
 * sample under the level originally chosen at the timeout value, and nothing
 * under the fallback level, whose measured time is dominated by the abandoned
 * wait (step 8). The decision of which level to run arrives with the request
 * (forced by the workload) until the rung scorer lands in stage 4.
 */
public final class KvClientService extends KvClientGrpc.KvClientImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(KvClientService.class);

    // Bound on every server-side wait (from config; see ExperimentConfig.chameleon).
    private static volatile long MAX_WAIT_MS = 400;

    public static void applyConfig(org.example.Utility.ExperimentConfig config) {
        MAX_WAIT_MS = config.chameleon.maxWaitMs;
    }

    private final ServerImpl node;
    private final MeasurementPlane plane;

    public KvClientService(ServerImpl node, MeasurementPlane plane) {
        this.node = node;
        this.plane = plane;
    }

    @Override
    public StreamObserver<KvRequest> session(StreamObserver<KvResponse> responseObserver) {
        return new StreamObserver<>() {
            @Override
            public void onNext(KvRequest request) {
                // Step 1: t_recv the moment the request comes off the stream,
                // before any queueing or waiting; the occupancy slot opens here.
                long tRecvNanos = System.nanoTime();
                plane.requestAdmitted();
                try {
                    handle(request, tRecvNanos, responseObserver);
                } catch (Exception e) {
                    LOG.error("Request {} failed on server {}", request.getRequestId(), node.nodeId(), e);
                    send(responseObserver, failure(request), tRecvNanos);
                }
            }

            @Override
            public void onError(Throwable t) {
                // Client went away; nothing to clean up, pending waits reply
                // into a cancelled stream and are dropped by send().
            }

            @Override
            public void onCompleted() {
                synchronized (responseObserver) {
                    responseObserver.onCompleted();
                }
            }
        };
    }

    private void handle(KvRequest request, long tRecvNanos, StreamObserver<KvResponse> out) {
        if (node.isDropAllServerNetworkTraffic()) {
            send(out, failure(request), tRecvNanos);
            return;
        }
        if (request.getIsRead()) {
            handleRead(request, tRecvNanos, out);
        } else {
            handleWrite(request, tRecvNanos, out);
        }
    }

    // ===== Reads =====

    private void handleRead(KvRequest request, long tRecvNanos, StreamObserver<KvResponse> out) {
        String key = request.getKey();
        switch (request.getForcedReadLevel()) {
            case EVENTUAL_LOCAL -> {
                fileMeasured(plane.readLevelIndex(ReadLevel.EVENTUAL_LOCAL), 0, tRecvNanos);
                send(out, readReply(request, ReadLevel.EVENTUAL_LOCAL, node.kv.readLocal(key), false, false),
                        tRecvNanos);
            }
            case EVENTUAL_MAJORITY -> {
                fileMeasured(plane.readLevelIndex(ReadLevel.EVENTUAL_MAJORITY), 0, tRecvNanos);
                send(out, readReply(request, ReadLevel.EVENTUAL_MAJORITY, node.kv.readCommitted(key), false, false),
                        tRecvNanos);
            }
            case CAUSAL_LOCAL -> {
                int anchor = request.getUncommittedSessionIndex();
                // Step 3: the gap this level must close, bucketed coarsely.
                // Everything at or below zero is one bucket.
                int gapBucket = ServiceTimeHistograms.gapBucketOf((long) anchor - node.lastLogIndex());
                int levelIndex = plane.readLevelIndex(ReadLevel.CAUSAL_LOCAL);
                if (anchor < 0 || node.lastLogIndex() >= anchor) {
                    fileMeasured(levelIndex, gapBucket, tRecvNanos);
                    send(out, readReply(request, ReadLevel.CAUSAL_LOCAL, node.kv.readLocal(key), false, false),
                            tRecvNanos);
                } else {
                    awaitBounded(node.awaitLocalLogIndex(anchor),
                            () -> {
                                fileMeasured(levelIndex, gapBucket, tRecvNanos);
                                send(out, readReply(request, ReadLevel.CAUSAL_LOCAL, node.kv.readLocal(key), true,
                                        false), tRecvNanos);
                            },
                            () -> {
                                // Abandoned wait: file under the chosen level at
                                // the timeout value, nothing under the fallback.
                                plane.fileServiceTime(levelIndex, gapBucket, MAX_WAIT_MS);
                                send(out, readReply(request, ReadLevel.EVENTUAL_LOCAL, node.kv.readLocal(key), true,
                                        true), tRecvNanos);
                            });
                }
            }
            case CAUSAL_MAJORITY -> {
                int anchor = request.getCommittedSessionIndex();
                int gapBucket = ServiceTimeHistograms.gapBucketOf((long) anchor - node.currentCommitIndex());
                int levelIndex = plane.readLevelIndex(ReadLevel.CAUSAL_MAJORITY);
                if (anchor < 0 || node.currentCommitIndex() >= anchor) {
                    fileMeasured(levelIndex, gapBucket, tRecvNanos);
                    send(out, readReply(request, ReadLevel.CAUSAL_MAJORITY, node.kv.readCommitted(key), false, false),
                            tRecvNanos);
                } else {
                    awaitBounded(node.awaitCommitIndex(anchor),
                            () -> {
                                fileMeasured(levelIndex, gapBucket, tRecvNanos);
                                send(out, readReply(request, ReadLevel.CAUSAL_MAJORITY, node.kv.readCommitted(key),
                                        true, false), tRecvNanos);
                            },
                            () -> {
                                plane.fileServiceTime(levelIndex, gapBucket, MAX_WAIT_MS);
                                send(out, readReply(request, ReadLevel.EVENTUAL_MAJORITY, node.kv.readCommitted(key),
                                        true, true), tRecvNanos);
                            });
                }
            }
            case LINEARIZABLE -> {
                if (!node.isLeader()) {
                    send(out, notLeader(request), tRecvNanos);
                    return;
                }
                int levelIndex = plane.readLevelIndex(ReadLevel.LINEARIZABLE);
                // ReadIndex: no log entry; serve committed state once a
                // majority heartbeat round confirms leadership. Bounded by the
                // same wait budget as every other level.
                node.confirmLeadership().orTimeout(MAX_WAIT_MS, TimeUnit.MILLISECONDS)
                        .whenCompleteAsync((readIndex, error) -> {
                            if (error == null) {
                                fileMeasured(levelIndex, 0, tRecvNanos);
                                send(out, readReply(request, ReadLevel.LINEARIZABLE, node.kv.readCommitted(key), true,
                                        false), tRecvNanos);
                            } else if (unwrap(error) instanceof ServerImpl.NotLeaderException) {
                                send(out, notLeader(request), tRecvNanos);
                            } else {
                                // Confirmation round timed out: abandoned-wait
                                // filing, then the strongest no-wait level.
                                plane.fileServiceTime(levelIndex, 0, MAX_WAIT_MS);
                                send(out, readReply(request, ReadLevel.EVENTUAL_MAJORITY, node.kv.readCommitted(key),
                                        true, true), tRecvNanos);
                            }
                        }, node.executorService);
            }
            default -> send(out, failure(request), tRecvNanos);
        }
    }

    // ===== Writes =====

    private void handleWrite(KvRequest request, long tRecvNanos, StreamObserver<KvResponse> out) {
        if (!node.isLeader()) {
            send(out, notLeader(request), tRecvNanos);
            return;
        }
        int writeConcern = Math.min(Math.max(1, request.getForcedWriteConcern()), node.majority());
        int levelIndex = plane.writeLevelIndex(writeConcern);

        int index;
        try {
            index = node.appendWrite(request.getKey(), request.getValue(),
                    request.getApplicationId() + "-" + request.getRequestId());
        } catch (ServerImpl.NotLeaderException e) {
            send(out, notLeader(request).setLeaderId(e.leaderId), tRecvNanos);
            return;
        }

        if (writeConcern <= 1) {
            fileMeasured(levelIndex, 0, tRecvNanos);
            send(out, writeReply(request, index, 1, false, false), tRecvNanos);
            return;
        }

        // wc:majority waits for commit (the loss window closes); intermediate
        // write concerns wait for the raw replica count.
        CompletableFuture<Void> wait = (writeConcern >= node.majority())
                ? node.awaitCommitIndex(index)
                : node.awaitReplication(index, writeConcern);
        awaitBounded(wait,
                () -> {
                    fileMeasured(levelIndex, 0, tRecvNanos);
                    send(out, writeReply(request, index, writeConcern, true, false), tRecvNanos);
                },
                () -> {
                    plane.fileServiceTime(levelIndex, 0, MAX_WAIT_MS);
                    send(out, writeReply(request, index, node.replicaCount(index), true, true), tRecvNanos);
                });
    }

    // ===== Helpers =====

    private void awaitBounded(CompletableFuture<Void> wait, Runnable onSatisfied, Runnable onExpired) {
        wait.orTimeout(MAX_WAIT_MS, TimeUnit.MILLISECONDS)
                .whenCompleteAsync((v, error) -> {
                    if (error == null) {
                        onSatisfied.run();
                    } else {
                        onExpired.run();
                    }
                }, node.executorService);
    }

    private void fileMeasured(int levelIndex, int gapBucket, long tRecvNanos) {
        plane.fileServiceTime(levelIndex, gapBucket, (System.nanoTime() - tRecvNanos) / 1_000_000.0);
    }

    private static Throwable unwrap(Throwable t) {
        return (t instanceof java.util.concurrent.CompletionException
                || t instanceof java.util.concurrent.ExecutionException) && t.getCause() != null ? t.getCause() : t;
    }

    private KvResponse.Builder readReply(KvRequest request, ReadLevel delivered, KvStore.Versioned versioned,
            boolean waited, boolean timedOutAndFellBack) {
        return KvResponse.newBuilder()
                .setRequestId(request.getRequestId())
                .setOk(true)
                .setValue(versioned == null ? "" : versioned.value())
                .setValueIndex(versioned == null ? -1 : versioned.index())
                .setDeliveredReadLevel(delivered)
                .setWaited(waited)
                .setTimedOutAndFellBack(timedOutAndFellBack);
    }

    private KvResponse.Builder writeReply(KvRequest request, int entryIndex, int deliveredWriteConcern,
            boolean waited, boolean timedOutAndFellBack) {
        return KvResponse.newBuilder()
                .setRequestId(request.getRequestId())
                .setOk(true)
                .setValueIndex(entryIndex)
                .setDeliveredWriteConcern(deliveredWriteConcern)
                .setWaited(waited)
                .setTimedOutAndFellBack(timedOutAndFellBack);
    }

    private KvResponse.Builder notLeader(KvRequest request) {
        return KvResponse.newBuilder()
                .setRequestId(request.getRequestId())
                .setOk(false)
                .setNotLeader(true)
                .setLeaderId(node.leaderIdHint());
    }

    private KvResponse.Builder failure(KvRequest request) {
        return KvResponse.newBuilder().setRequestId(request.getRequestId()).setOk(false).setLeaderId(-1);
    }

    private void send(StreamObserver<KvResponse> out, KvResponse.Builder reply, long tRecvNanos) {
        double serviceTimeMs = (System.nanoTime() - tRecvNanos) / 1_000_000.0;
        reply.setLogIndex(node.lastLogIndex())
                .setCommitIndex(node.currentCommitIndex())
                .setServiceTimeMs(serviceTimeMs);
        // The slot closes exactly once per request: every handle path ends in
        // exactly one send.
        plane.requestCompleted(serviceTimeMs);
        try {
            synchronized (out) {
                out.onNext(reply.build());
            }
        } catch (RuntimeException e) {
            // Stream cancelled by the client; the reply has nowhere to go.
            LOG.debug("Dropping reply for request {}: {}", reply.getRequestId(), e.toString());
        }
    }
}
