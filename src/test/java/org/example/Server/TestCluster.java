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
 * A real 3-node cluster with gRPC Raft and framed client ingress per node.
 * Each test uses its own port range so tests never collide with each other
 * or with a concurrently running experiment.
 */
final class TestCluster implements AutoCloseable {

    static final int NUM_NODES = 3;

    final List<ServerImpl> nodes = new ArrayList<>();
    final List<MeasurementPlane> planes = new ArrayList<>();
    final List<KvClientService> clientServices = new ArrayList<>();
    final List<Server> rpcServers = new ArrayList<>();
    final List<KvIngressServer> ingressServers = new ArrayList<>();
    final int basePort;
    final int clientBasePort;

    TestCluster(int basePort) throws IOException {
        this.basePort = basePort;
        this.clientBasePort = basePort + 10_000;
        ServerImpl.applyClusterSettings(Collections.nCopies(NUM_NODES, "localhost"), basePort);
        for (int i = 0; i < NUM_NODES; i++) {
            ServerImpl node = new ServerImpl(i, NUM_NODES);
            MeasurementPlane plane = new MeasurementPlane(i, NUM_NODES);
            KvClientService clientService = new KvClientService(node, plane);
            KvIngressServer ingress = new KvIngressServer(clientBasePort + i + 1, clientService, plane);
            ingress.start();
            ingressServers.add(ingress);
            rpcServers.add(ServerBuilder.forPort(basePort + i + 1)
                    .addService(node)
                    .build().start());
            nodes.add(node);
            planes.add(plane);
            clientServices.add(clientService);
        }
        for (ServerImpl node : nodes) {
            node.setUpStubs();
        }
    }

    /** Framed client ingress port node i listens on. */
    int portOf(int nodeId) {
        return clientBasePort + nodeId + 1;
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

    /**
     * Wait until a follower has learned the leader from a heartbeat. Right
     * after an election a follower's leader hint can still be -1, so tests
     * that make the follower contact the leader (read-index rounds) must not
     * race the first AppendEntries.
     */
    void awaitLeaderHint(ServerImpl follower, ServerImpl leader, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (follower.leaderIdHint() == leader.nodeId()) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("follower " + follower.nodeId() + " never learned leader " + leader.nodeId());
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
        for (KvIngressServer ingress : ingressServers) {
            ingress.close();
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
        for (KvClientService clientService : clientServices) {
            clientService.close();
        }
        for (ServerImpl node : nodes) {
            node.shutdown();
        }
        for (MeasurementPlane plane : planes) {
            plane.close();
        }
    }
}
