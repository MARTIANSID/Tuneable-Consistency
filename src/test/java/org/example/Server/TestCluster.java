package org.example.Server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.example.Utility.ServerStatus.ServerCurrentStatus;

import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * A real 3-node cluster on localhost gRPC for integration tests, with both
 * the Raft service and the client-facing KV service registered per node.
 * Each test uses its own port range so tests never collide with each other
 * or with a concurrently running experiment.
 */
final class TestCluster implements AutoCloseable {

    static final int NUM_NODES = 3;

    final List<ServerImpl> nodes = new ArrayList<>();
    final List<Server> rpcServers = new ArrayList<>();
    final int basePort;

    TestCluster(int basePort) throws IOException {
        this.basePort = basePort;
        ServerImpl.applyClusterSettings(Collections.nCopies(NUM_NODES, "localhost"), basePort);
        for (int i = 0; i < NUM_NODES; i++) {
            ServerImpl node = new ServerImpl(i, NUM_NODES);
            rpcServers.add(ServerBuilder.forPort(basePort + i + 1)
                    .addService(node)
                    .addService(new KvClientService(node))
                    .build().start());
            nodes.add(node);
        }
        for (ServerImpl node : nodes) {
            node.setUpStubs();
        }
    }

    /** gRPC port node i listens on. */
    int portOf(int nodeId) {
        return basePort + nodeId + 1;
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
                if (!node.isDropAllServerNetworkTraffic() && node.currentCommitIndex() < index) {
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
            state.append(" node").append(node.nodeId()).append("=").append(node.currentCommitIndex());
        }
        throw new AssertionError("commit index " + index + " not reached within " + timeoutMs + " ms:" + state);
    }

    /** Append a write directly on the leader, retrying across leader changes. */
    ServerImpl append(ServerImpl leader, String key, String value, String id, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            try {
                leader.appendWrite(key, value, id);
                return leader;
            } catch (ServerImpl.NotLeaderException e) {
                if (System.currentTimeMillis() >= deadline) {
                    throw new AssertionError("write " + id + " not accepted within " + timeoutMs + " ms");
                }
                leader = awaitLeader(timeoutMs);
            }
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
