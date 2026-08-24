package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class ReplicationPipelineTest {

    @Test
    void boundedOrderedPipelineRespectsConfiguredLimitsAndRecoversAStream() throws Exception {
        ServerImpl.applyReplicationSettingsForTest(2, 4);
        try (TestCluster cluster = new TestCluster(19800)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            ServerImpl disconnectedFollower = cluster.nodes.stream()
                    .filter(node -> node != leader)
                    .findFirst()
                    .orElseThrow();

            disconnectedFollower.setDropAllServerNetworkTraffic(true);
            int lastIndex = -1;
            for (int i = 0; i < 100; i++) {
                lastIndex = leader.appendWrite("pipeline-" + (i % 10), "v" + i, "pipeline-op-" + i);
            }
            leader.awaitCommitIndex(lastIndex).get(15, TimeUnit.SECONDS);

            assertTrue(leader.maxObservedReplicationBatchEntries() <= 2,
                    "no stream message may exceed the configured entry cap");
            boolean observedPipelining = false;
            for (ServerImpl follower : cluster.nodes) {
                if (follower != leader) {
                    int maxInflight = leader.maxObservedReplicationInflight(follower.nodeId());
                    assertTrue(maxInflight <= 4,
                            "the speculative per-follower window must remain bounded");
                    observedPipelining |= maxInflight > 1;
                }
            }
            assertTrue(observedPipelining,
                    "the leader should send another batch before the preceding batch is acknowledged");

            disconnectedFollower.setDropAllServerNetworkTraffic(false);
            cluster.awaitCommitted(lastIndex, 15_000);
            assertEquals(lastIndex, disconnectedFollower.currentCommitIndex());
            assertEquals("v99", disconnectedFollower.kv.readCommitted("pipeline-9").value());
        } finally {
            ServerImpl.applyReplicationSettingsForTest(4000, 4);
        }
    }
}
