package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.example.Utility.ServerStatus.ServerCurrentStatus;
import org.example.Utility.TransactionOption;
import org.example.raft.ClientMessage;
import org.example.raft.TimeStampProto;
import org.example.raft.Transaction;
import org.junit.jupiter.api.Test;

import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * Integration tests over a real 3-node cluster on localhost gRPC. Each test
 * uses its own port range so tests never collide with each other or with a
 * concurrently running experiment. Admission control is disabled so no Redis
 * is required; writes enter through the same enqueue-and-batch path the
 * experiment uses.
 */
class RaftClusterTest {

    private static final int NUM_NODES = 3;

    private static final class TestCluster implements AutoCloseable {
        final List<ServerImpl> nodes = new ArrayList<>();
        final List<Server> rpcServers = new ArrayList<>();

        TestCluster(int basePort) throws IOException {
            ServerImpl.applyClusterSettings(Collections.nCopies(NUM_NODES, "localhost"), basePort);
            // No Redis in unit tests: the token bucket is only touched when
            // admission control or upgrades are on.
            ServerImpl.setAdmissionControlEnabled(false);
            ServerImpl.setUpgradeTransactionsEnabled(false);
            ServerImpl.setPressureModeEnabled(false);
            for (int i = 0; i < NUM_NODES; i++) {
                ServerImpl node = new ServerImpl(i, NUM_NODES);
                rpcServers.add(ServerBuilder.forPort(basePort + i + 1).addService(node).build().start());
                nodes.add(node);
            }
            for (ServerImpl node : nodes) {
                node.setUpStubs();
            }
        }

        ServerImpl awaitLeader(long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                for (ServerImpl node : nodes) {
                    if (node.status == ServerCurrentStatus.LEADER && !node.isDropAllServerNetworkTraffic()) {
                        return node;
                    }
                }
                Thread.sleep(20);
            }
            throw new AssertionError("no leader elected within " + timeoutMs + " ms");
        }

        /** Wait until every live (non-dropped) node has committed up to index. */
        void awaitCommitted(int index, long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                boolean allCaughtUp = true;
                for (ServerImpl node : nodes) {
                    if (!node.isDropAllServerNetworkTraffic() && node.commitIndex.get() < index) {
                        allCaughtUp = false;
                        break;
                    }
                }
                if (allCaughtUp) {
                    return;
                }
                Thread.sleep(20);
            }
            StringBuilder state = new StringBuilder();
            for (ServerImpl node : nodes) {
                state.append(" node").append(nodes.indexOf(node)).append("=").append(node.commitIndex.get());
            }
            throw new AssertionError("commit index " + index + " not reached within " + timeoutMs + " ms:" + state);
        }

        /** Append a write through the regular enqueue path, retrying across leader changes. */
        ServerImpl append(ServerImpl leader, String key, String value, String id, long timeoutMs)
                throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (true) {
                int accepted = leader.enqueueWithoutDroppingExisting(
                        List.of(TransactionOption.fromClientMessage(writeMessage(key, value, id))));
                if (accepted == 1) {
                    return leader;
                }
                if (System.currentTimeMillis() >= deadline) {
                    throw new AssertionError("write " + id + " not accepted within " + timeoutMs + " ms");
                }
                leader = awaitLeader(timeoutMs);
            }
        }

        @Override
        public void close() {
            for (ServerImpl node : nodes) {
                node.shutdown();
            }
            for (Server server : rpcServers) {
                server.shutdownNow();
            }
            for (Server server : rpcServers) {
                try {
                    server.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private static ClientMessage writeMessage(String key, String value, String id) {
        long now = System.currentTimeMillis();
        Transaction t = Transaction.newBuilder()
                .setId(id)
                .setKey(key)
                .setValue(value)
                .setIsReadOnly(false)
                .setMinRequiredConsistency(1)
                .setWriteConcern(1)
                .setTransactionSendTimeInMs(now)
                .build();
        return ClientMessage.newBuilder()
                .setT(t)
                .setWriteConcern(1)
                .setTimeStamp(TimeStampProto.newBuilder().setP(now).setL(0).build())
                .setCallbackHost("localhost")
                .setCallbackPort(19999)
                .build();
    }

    @Test
    void electsLeaderReplicatesAndAppliesIdentically() throws Exception {
        try (TestCluster cluster = new TestCluster(18100)) {
            ServerImpl leader = cluster.awaitLeader(15_000);

            int entries = 200;
            for (int i = 0; i < entries; i++) {
                leader = cluster.append(leader, "k" + (i % 20), "v" + i, "op" + i, 15_000);
            }
            cluster.awaitCommitted(entries - 1, 15_000);

            for (int n = 0; n < NUM_NODES; n++) {
                KvStore kv = cluster.nodes.get(n).kv;
                for (int k = 0; k < 20; k++) {
                    KvStore.Versioned reference = cluster.nodes.get(0).kv.readCommitted("k" + k);
                    KvStore.Versioned onNode = kv.readCommitted("k" + k);
                    assertNotNull(onNode, "k" + k + " missing on node " + n);
                    assertEquals(reference.value(), onNode.value(), "committed value of k" + k + " on node " + n);
                    assertEquals(reference.index(), onNode.index(), "committed index of k" + k + " on node " + n);
                }
            }
            // Writes were appended in order, so the last write to k19 was v199.
            assertEquals("v199", cluster.nodes.get(0).kv.readCommitted("k19").value());
        }
    }

    @Test
    void reElectsAfterLeaderCrashAndKeepsCommittedState() throws Exception {
        try (TestCluster cluster = new TestCluster(18200)) {
            ServerImpl leader = cluster.awaitLeader(15_000);

            int firstBatch = 100;
            for (int i = 0; i < firstBatch; i++) {
                leader = cluster.append(leader, "k" + i, "v" + i, "op" + i, 15_000);
            }
            cluster.awaitCommitted(firstBatch - 1, 15_000);

            // Crash the leader: it stops sending and answering all Raft RPCs.
            ServerImpl crashed = leader;
            crashed.setDropAllServerNetworkTraffic(true);

            ServerImpl newLeader = cluster.awaitLeader(20_000);
            assertNotEquals(crashed, newLeader, "a different node must take over");

            int secondBatch = 50;
            for (int i = 0; i < secondBatch; i++) {
                newLeader = cluster.append(newLeader, "post" + i, "w" + i, "post-op" + i, 20_000);
            }
            cluster.awaitCommitted(firstBatch + secondBatch - 1, 20_000);

            for (ServerImpl node : cluster.nodes) {
                if (node == crashed) {
                    continue;
                }
                assertEquals("v99", node.kv.readCommitted("k99").value(),
                        "pre-crash committed state must survive on node " + cluster.nodes.indexOf(node));
                assertEquals("w49", node.kv.readCommitted("post49").value(),
                        "post-crash writes must replicate on node " + cluster.nodes.indexOf(node));
            }
        }
    }

    @Test
    void confirmLeadershipReturnsCoveringReadIndexAndRejectsFollowers() throws Exception {
        try (TestCluster cluster = new TestCluster(18300)) {
            ServerImpl leader = cluster.awaitLeader(15_000);

            for (int i = 0; i < 10; i++) {
                leader = cluster.append(leader, "k", "v" + i, "op" + i, 15_000);
            }
            cluster.awaitCommitted(9, 15_000);

            int readIndex = leader.confirmLeadership().get(5, TimeUnit.SECONDS);
            assertTrue(readIndex >= 9, "read index must cover committed entries, got " + readIndex);

            ServerImpl follower = null;
            for (ServerImpl node : cluster.nodes) {
                if (node != leader) {
                    follower = node;
                    break;
                }
            }
            assertNotNull(follower);
            ExecutionException rejection = assertThrows(ExecutionException.class,
                    follower.confirmLeadership()::get);
            assertInstanceOf(ServerImpl.NotLeaderException.class, rejection.getCause());
        }
    }
}
