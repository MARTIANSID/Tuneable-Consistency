package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

/**
 * Integration tests for the Raft core over a real 3-node cluster on localhost
 * gRPC. Writes enter through ServerImpl.appendWrite, the same mechanic the
 * client-facing service uses.
 */
class RaftClusterTest {

    @Test
    void electsLeaderReplicatesAndAppliesIdentically() throws Exception {
        try (TestCluster cluster = new TestCluster(18100)) {
            ServerImpl leader = cluster.awaitLeader(15_000);

            int entries = 200;
            for (int i = 0; i < entries; i++) {
                leader = cluster.append(leader, "k" + (i % 20), "v" + i, "op" + i, 15_000);
            }
            cluster.awaitCommitted(entries - 1, 15_000);

            for (int n = 0; n < cluster.nodes.size(); n++) {
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
                        "pre-crash committed state must survive on node " + node.nodeId());
                assertEquals("w49", node.kv.readCommitted("post49").value(),
                        "post-crash writes must replicate on node " + node.nodeId());
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
