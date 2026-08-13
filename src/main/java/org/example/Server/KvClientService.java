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
 * The client-facing per-request path (Chameleon stage 2). Every request is
 * timestamped the moment it comes off the stream (t_recv, monotonic clock),
 * executed at its forced level, and answered on the same stream with the
 * delivered level, the node's log/commit indices, and the measured service
 * time.
 *
 * Levels are pure mechanics here: eventual levels read a view immediately,
 * causal levels wait for the relevant index to reach the client's session
 * anchor, linearizable runs a ReadIndex confirmation on the leader, and write
 * concerns wait for replication or commit. Waits are bounded by maxWaitMs;
 * on expiry the request falls back to the strongest level that needs no
 * waiting and says so in the response. The decision of which level to run
 * arrives with the request (forced by the workload) until the rung scorer
 * lands in stage 4.
 */
public final class KvClientService extends KvClientGrpc.KvClientImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(KvClientService.class);

    // Bound on every server-side wait (from config; see ExperimentConfig.chameleon).
    private static volatile long MAX_WAIT_MS = 400;

    public static void applyConfig(org.example.Utility.ExperimentConfig config) {
        MAX_WAIT_MS = config.chameleon.maxWaitMs;
    }

    private final ServerImpl node;

    public KvClientService(ServerImpl node) {
        this.node = node;
    }

    @Override
    public StreamObserver<KvRequest> session(StreamObserver<KvResponse> responseObserver) {
        return new StreamObserver<>() {
            @Override
            public void onNext(KvRequest request) {
                // Step 1: t_recv the moment the request comes off the stream,
                // before any queueing or waiting.
                long tRecvNanos = System.nanoTime();
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
            case EVENTUAL_LOCAL ->
                send(out, readReply(request, ReadLevel.EVENTUAL_LOCAL, node.kv.readLocal(key), false, false),
                        tRecvNanos);
            case EVENTUAL_MAJORITY ->
                send(out, readReply(request, ReadLevel.EVENTUAL_MAJORITY, node.kv.readCommitted(key), false, false),
                        tRecvNanos);
            case CAUSAL_LOCAL -> {
                int anchor = request.getUncommittedSessionIndex();
                if (anchor < 0 || node.lastLogIndex() >= anchor) {
                    send(out, readReply(request, ReadLevel.CAUSAL_LOCAL, node.kv.readLocal(key), false, false),
                            tRecvNanos);
                } else {
                    awaitBounded(node.awaitLocalLogIndex(anchor),
                            () -> send(out,
                                    readReply(request, ReadLevel.CAUSAL_LOCAL, node.kv.readLocal(key), true, false),
                                    tRecvNanos),
                            () -> send(out,
                                    readReply(request, ReadLevel.EVENTUAL_LOCAL, node.kv.readLocal(key), true, true),
                                    tRecvNanos));
                }
            }
            case CAUSAL_MAJORITY -> {
                int anchor = request.getCommittedSessionIndex();
                if (anchor < 0 || node.currentCommitIndex() >= anchor) {
                    send(out, readReply(request, ReadLevel.CAUSAL_MAJORITY, node.kv.readCommitted(key), false, false),
                            tRecvNanos);
                } else {
                    awaitBounded(node.awaitCommitIndex(anchor),
                            () -> send(out,
                                    readReply(request, ReadLevel.CAUSAL_MAJORITY, node.kv.readCommitted(key), true,
                                            false),
                                    tRecvNanos),
                            () -> send(out,
                                    readReply(request, ReadLevel.EVENTUAL_MAJORITY, node.kv.readCommitted(key), true,
                                            true),
                                    tRecvNanos));
                }
            }
            case LINEARIZABLE -> {
                if (!node.isLeader()) {
                    send(out, notLeader(request), tRecvNanos);
                    return;
                }
                // ReadIndex: no log entry; serve committed state once a
                // majority heartbeat round confirms leadership. The committed
                // view covers the read index by construction.
                node.confirmLeadership().whenCompleteAsync((readIndex, error) -> {
                    if (error == null) {
                        send(out, readReply(request, ReadLevel.LINEARIZABLE, node.kv.readCommitted(key), true, false),
                                tRecvNanos);
                    } else if (unwrap(error) instanceof ServerImpl.NotLeaderException) {
                        send(out, notLeader(request), tRecvNanos);
                    } else {
                        // Confirmation round timed out: fall back to the
                        // strongest no-wait level.
                        send(out, readReply(request, ReadLevel.EVENTUAL_MAJORITY, node.kv.readCommitted(key), true,
                                true), tRecvNanos);
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

        int index;
        try {
            index = node.appendWrite(request.getKey(), request.getValue(),
                    request.getApplicationId() + "-" + request.getRequestId());
        } catch (ServerImpl.NotLeaderException e) {
            send(out, notLeader(request).setLeaderId(e.leaderId), tRecvNanos);
            return;
        }

        if (writeConcern <= 1) {
            send(out, writeReply(request, index, 1, false, false), tRecvNanos);
            return;
        }

        // wc:majority waits for commit (the loss window closes); intermediate
        // write concerns wait for the raw replica count.
        CompletableFuture<Void> wait = (writeConcern >= node.majority())
                ? node.awaitCommitIndex(index)
                : node.awaitReplication(index, writeConcern);
        awaitBounded(wait,
                () -> send(out, writeReply(request, index, writeConcern, true, false), tRecvNanos),
                () -> send(out, writeReply(request, index, node.replicaCount(index), true, true), tRecvNanos));
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
        reply.setLogIndex(node.lastLogIndex())
                .setCommitIndex(node.currentCommitIndex())
                .setServiceTimeMs((System.nanoTime() - tRecvNanos) / 1_000_000.0);
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
