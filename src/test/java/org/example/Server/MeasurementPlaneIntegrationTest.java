package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.example.raft.KvClientGrpc;
import org.example.raft.KvRequest;
import org.example.raft.KvResponse;
import org.example.raft.ReadLevel;
import org.junit.jupiter.api.Test;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

/**
 * The measurement plane against a live cluster: cells populate under real
 * traffic, the abandoned-wait rule files at the timeout value under the
 * chosen level, and the occupancy integral matches the sum of completed
 * service times (they measure the same quantity, so with nothing in flight
 * they must agree tightly).
 */
class MeasurementPlaneIntegrationTest {

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

        @Override
        public void close() {
            channel.shutdownNow();
        }
    }

    @Test
    void cellsPopulateAndOccupancyMatchesCompletedServiceTimes() throws Exception {
        try (TestCluster cluster = new TestCluster(18900)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            MeasurementPlane plane = cluster.planes.get(leader.nodeId());

            try (TestSession session = new TestSession(cluster.portOf(leader.nodeId()))) {
                for (int i = 0; i < 300; i++) {
                    session.call(KvRequest.newBuilder().setIsRead(false).setKey("k" + (i % 10)).setValue("v" + i)
                            .setForcedWriteConcern((i % 2 == 0) ? 1 : 2)
                            .setCommittedSessionIndex(-1).setUncommittedSessionIndex(-1));
                    session.call(KvRequest.newBuilder().setIsRead(true).setKey("k" + (i % 10))
                            .setForcedReadLevel((i % 2 == 0) ? ReadLevel.EVENTUAL_LOCAL : ReadLevel.LINEARIZABLE)
                            .setCommittedSessionIndex(-1).setUncommittedSessionIndex(-1));
                }

                // Let the refresh tick publish and the last replies settle.
                Thread.sleep(400);

                ServiceTimeHistograms h = plane.histograms();
                assertTrue(h.snapshot(plane.readLevelIndex(ReadLevel.EVENTUAL_LOCAL), 0).totalCount > 0,
                        "eventual-local cell must populate");
                assertTrue(h.snapshot(plane.readLevelIndex(ReadLevel.LINEARIZABLE), 0).totalCount > 0,
                        "linearizable cell must populate");
                assertTrue(h.snapshot(plane.writeLevelIndex(1), 0).totalCount > 0, "wc:1 cell must populate");
                assertTrue(h.snapshot(plane.writeLevelIndex(2), 0).totalCount > 0, "wc:2 cell must populate");

                // Linearizable pays a confirmation round; its mean service
                // time must exceed the immediate eventual-local reads.
                double linMean = h.snapshot(plane.readLevelIndex(ReadLevel.LINEARIZABLE), 0).meanMs;
                double eventualMean = h.snapshot(plane.readLevelIndex(ReadLevel.EVENTUAL_LOCAL), 0).meanMs;
                assertTrue(linMean > eventualMean,
                        "LIN mean " + linMean + " must exceed eventual mean " + eventualMean);

                // Nothing in flight now, so the occupancy integral and the sum
                // of completed service times measure exactly the same thing.
                assertEquals(0, plane.inFlight(), "all requests replied");
                double slotMs = plane.cumulativeSlotNanos() / 1_000_000.0;
                double completedMs = plane.cumulativeCompletedServiceMs();
                assertTrue(completedMs > 0);
                // The open tail (since the last interval close) is not yet in
                // cumulativeSlotNanos, so allow a proportional margin.
                assertEquals(1.0, slotMs / completedMs, 0.15,
                        "occupancy integral " + slotMs + " ms vs completed service " + completedMs + " ms");
            }
        }
    }

    @Test
    void abandonedWaitFilesAtTheTimeoutUnderTheChosenLevel() throws Exception {
        try (TestCluster cluster = new TestCluster(19000)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            ServerImpl follower = cluster.nodes.stream().filter(n -> n != leader).findFirst().orElseThrow();
            MeasurementPlane plane = cluster.planes.get(follower.nodeId());

            try (TestSession session = new TestSession(cluster.portOf(follower.nodeId()))) {
                // Unreachable anchor in the far-behind gap bucket: the wait
                // expires, the fallback serves, and the sample lands in the
                // causal-local far-gap cell at the timeout value.
                int unreachable = follower.lastLogIndex() + 100_000;
                KvResponse read = session.call(KvRequest.newBuilder().setIsRead(true).setKey("any")
                        .setForcedReadLevel(ReadLevel.CAUSAL_LOCAL)
                        .setCommittedSessionIndex(-1).setUncommittedSessionIndex(unreachable));
                assertTrue(read.getTimedOutAndFellBack());

                Thread.sleep(300); // let the tick publish

                int causalLocal = plane.readLevelIndex(ReadLevel.CAUSAL_LOCAL);
                ServiceTimeHistograms.Snapshot farGap = plane.histograms().snapshot(causalLocal, 3);
                assertTrue(farGap.totalCount > 0, "abandoned wait must file under the chosen level's cell");
                assertEquals(400.0, farGap.meanMs, 1.0,
                        "abandoned wait files at the timeout value (maxWaitMs), got " + farGap.meanMs);

                // Nothing under the fallback level: its cell must not have
                // absorbed the abandoned wait.
                ServiceTimeHistograms.Snapshot fallbackCell = plane.histograms()
                        .snapshot(plane.readLevelIndex(ReadLevel.EVENTUAL_LOCAL), 0);
                assertEquals(0.0, fallbackCell.totalCount, 1e-9,
                        "the fallback level's cell must stay empty for an abandoned wait");
            }
        }
    }
}
