package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.example.Client.KvSessionClient;
import org.example.raft.KvClientGrpc;
import org.example.raft.KvRequest;
import org.example.raft.KvResponse;
import org.example.raft.ReadLevel;
import org.junit.jupiter.api.Test;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

/**
 * Level mechanics over the real client protocol: every read level and write
 * concern exercised through gRPC session streams against a live cluster,
 * including wait-timeout fallback, leader redirects, and the session
 * guarantee assertions of the real client under load.
 */
class KvLevelMechanicsTest {

    /** Minimal blocking session for tests: send one request, await its response. */
    private static final class TestSession implements AutoCloseable {
        private final ManagedChannel channel;
        private final StreamObserver<KvRequest> stream;
        private final ConcurrentHashMap<Long, CompletableFuture<KvResponse>> waiting = new ConcurrentHashMap<>();
        private final AtomicLong ids = new AtomicLong();

        TestSession(int port) {
            channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
            stream = KvClientGrpc.newStub(channel).session(new StreamObserver<>() {
                @Override
                public void onNext(KvResponse response) {
                    CompletableFuture<KvResponse> future = waiting.remove(response.getRequestId());
                    if (future != null) {
                        future.complete(response);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    waiting.values().forEach(f -> f.completeExceptionally(t));
                }

                @Override
                public void onCompleted() {
                }
            });
        }

        KvResponse call(KvRequest.Builder request) throws Exception {
            long id = ids.incrementAndGet();
            CompletableFuture<KvResponse> future = new CompletableFuture<>();
            waiting.put(id, future);
            synchronized (stream) {
                stream.onNext(request.setRequestId(id).build());
            }
            return future.get(10, TimeUnit.SECONDS);
        }

        KvResponse write(String key, String value, int writeConcern) throws Exception {
            return call(KvRequest.newBuilder().setIsRead(false).setKey(key).setValue(value)
                    .setForcedWriteConcern(writeConcern)
                    .setCommittedSessionIndex(-1).setUncommittedSessionIndex(-1));
        }

        KvResponse read(String key, ReadLevel level, int committedAnchor, int uncommittedAnchor) throws Exception {
            return call(KvRequest.newBuilder().setIsRead(true).setKey(key)
                    .setForcedReadLevel(level)
                    .setCommittedSessionIndex(committedAnchor).setUncommittedSessionIndex(uncommittedAnchor));
        }

        @Override
        public void close() {
            channel.shutdownNow();
        }
    }

    @Test
    void writeConcernsAndEventualViews() throws Exception {
        try (TestCluster cluster = new TestCluster(18400)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            try (TestSession session = new TestSession(cluster.portOf(leader.nodeId()))) {

                // wc:1 acknowledges at append, before commit.
                KvResponse w1 = session.write("k1", "va", 1);
                assertTrue(w1.getOk());
                assertEquals(1, w1.getDeliveredWriteConcern());
                assertTrue(w1.getValueIndex() >= 0);
                assertFalse(w1.getTimedOutAndFellBack());

                // wc:majority acknowledges at commit: the response's commit
                // index must already cover the entry.
                KvResponse w2 = session.write("k2", "vb", 2);
                assertTrue(w2.getOk());
                assertEquals(2, w2.getDeliveredWriteConcern());
                assertFalse(w2.getTimedOutAndFellBack());
                assertTrue(w2.getCommitIndex() >= w2.getValueIndex(),
                        "majority ack implies committed: commitIndex=" + w2.getCommitIndex()
                                + " entry=" + w2.getValueIndex());

                // Eventual-local on the leader sees both immediately.
                KvResponse local = session.read("k1", ReadLevel.EVENTUAL_LOCAL, -1, -1);
                assertEquals("va", local.getValue());
                assertEquals(ReadLevel.EVENTUAL_LOCAL, local.getDeliveredReadLevel());
                assertFalse(local.getWaited());

                // Eventual-majority sees the majority-committed write.
                KvResponse committed = session.read("k2", ReadLevel.EVENTUAL_MAJORITY, -1, -1);
                assertEquals("vb", committed.getValue());
                assertEquals(ReadLevel.EVENTUAL_MAJORITY, committed.getDeliveredReadLevel());
            }
        }
    }

    @Test
    void causalMajorityWaitsForCommitOnFollower() throws Exception {
        try (TestCluster cluster = new TestCluster(18500)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            ServerImpl follower = cluster.nodes.stream().filter(n -> n != leader).findFirst().orElseThrow();

            try (TestSession leaderSession = new TestSession(cluster.portOf(leader.nodeId()));
                    TestSession followerSession = new TestSession(cluster.portOf(follower.nodeId()))) {

                // wc:1 write: acknowledged before commit; its index anchors the
                // causal-majority read, which must block until the commit
                // index catches up on the follower.
                KvResponse write = leaderSession.write("causal-key", "cv", 1);
                int anchor = write.getValueIndex();

                KvResponse read = followerSession.read("causal-key", ReadLevel.CAUSAL_MAJORITY, anchor, -1);
                assertTrue(read.getOk());
                assertEquals(ReadLevel.CAUSAL_MAJORITY, read.getDeliveredReadLevel());
                assertFalse(read.getTimedOutAndFellBack());
                assertEquals("cv", read.getValue());
                assertTrue(read.getValueIndex() >= anchor);
                assertTrue(read.getCommitIndex() >= anchor);
            }
        }
    }

    @Test
    void causalWaitTimesOutAndFallsBackToEventual() throws Exception {
        try (TestCluster cluster = new TestCluster(18600)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            ServerImpl follower = cluster.nodes.stream().filter(n -> n != leader).findFirst().orElseThrow();

            try (TestSession session = new TestSession(cluster.portOf(follower.nodeId()))) {
                // Anchor far beyond anything that will ever be written: the
                // bounded wait must expire and fall back to eventual-local.
                int unreachable = follower.lastLogIndex() + 100_000;
                long start = System.nanoTime();
                KvResponse read = session.read("any", ReadLevel.CAUSAL_LOCAL, -1, unreachable);
                double elapsedMs = (System.nanoTime() - start) / 1_000_000.0;

                assertTrue(read.getOk());
                assertTrue(read.getWaited());
                assertTrue(read.getTimedOutAndFellBack());
                assertEquals(ReadLevel.EVENTUAL_LOCAL, read.getDeliveredReadLevel(),
                        "fallback must be the strongest no-wait level of the same view");
                assertTrue(elapsedMs >= 350, "the bounded wait must actually run, took " + elapsedMs + " ms");
            }
        }
    }

    @Test
    void linearizableIsLeaderOnlyAndServesCommittedState() throws Exception {
        try (TestCluster cluster = new TestCluster(18700)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            ServerImpl follower = cluster.nodes.stream().filter(n -> n != leader).findFirst().orElseThrow();

            try (TestSession leaderSession = new TestSession(cluster.portOf(leader.nodeId()));
                    TestSession followerSession = new TestSession(cluster.portOf(follower.nodeId()))) {

                KvResponse write = leaderSession.write("lin-key", "lv", 2);
                assertTrue(write.getOk());

                // On a follower: redirect with a leader hint.
                KvResponse redirected = followerSession.read("lin-key", ReadLevel.LINEARIZABLE, -1, -1);
                assertFalse(redirected.getOk());
                assertTrue(redirected.getNotLeader());
                assertEquals(leader.nodeId(), redirected.getLeaderId());

                // On the leader: ReadIndex round, served from committed state.
                KvResponse read = leaderSession.read("lin-key", ReadLevel.LINEARIZABLE, -1, -1);
                assertTrue(read.getOk());
                assertEquals(ReadLevel.LINEARIZABLE, read.getDeliveredReadLevel());
                assertEquals("lv", read.getValue());
                assertFalse(read.getTimedOutAndFellBack());
                assertTrue(read.getWaited(), "linearizable always waits for the confirmation round");
            }
        }
    }

    @Test
    void sessionClientKeepsReadYourWritesUnderLoad() throws Exception {
        try (TestCluster cluster = new TestCluster(18800)) {
            cluster.awaitLeader(15_000);

            try (KvSessionClient client = new KvSessionClient(1,
                    List.of("localhost", "localhost", "localhost"), 18800, 64, 3, 8_000)) {

                // Interleave writes and causal reads over a small keyspace so
                // reads constantly chase this session's own writes across all
                // three nodes (round-robin), including majority-anchored reads.
                for (int i = 0; i < 300; i++) {
                    String key = "s" + (i % 10);
                    client.sendWrite(key, "v" + i, (i % 2 == 0) ? 1 : 2);
                    client.sendRead(key, (i % 2 == 0) ? ReadLevel.CAUSAL_LOCAL : ReadLevel.CAUSAL_MAJORITY);
                    if (i % 5 == 0) {
                        client.sendRead(key, ReadLevel.LINEARIZABLE);
                    }
                }

                long deadline = System.currentTimeMillis() + 20_000;
                while (client.pendingCount() > 0 && System.currentTimeMillis() < deadline) {
                    Thread.sleep(50);
                }
                assertEquals(0, client.pendingCount(), "all requests must resolve");
                assertEquals(0, client.sessionViolations(),
                        "session guarantees (read-your-writes) must hold under load");
            }
        }
    }
}
