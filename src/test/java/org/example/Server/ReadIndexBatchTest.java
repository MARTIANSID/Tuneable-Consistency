package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

/**
 * Follower read-index batching: a burst of concurrent linearizable reads on a
 * follower shares confirmation rounds instead of issuing one leader RPC per
 * read. Requests arriving before a round's send share its index; requests
 * arriving while a round is in flight join the next one - so a burst issued
 * within one round's flight time needs at most two rounds.
 */
class ReadIndexBatchTest {

    @Test
    void concurrentFollowerReadsShareConfirmationRounds() throws Exception {
        try (TestCluster cluster = new TestCluster(19850)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            ServerImpl follower = cluster.nodes.stream().filter(n -> n != leader).findFirst().orElseThrow();
            cluster.awaitLeaderHint(follower, leader, 5_000);

            cluster.append(leader, "b-key", "b-value", "b-op", 5_000);
            int committedFloor = leader.currentCommitIndex();

            int burst = 40;
            List<CompletableFuture<Integer>> futures = new ArrayList<>();
            for (int i = 0; i < burst; i++) {
                futures.add(follower.readIndexFromLeader());
            }
            for (CompletableFuture<Integer> future : futures) {
                int readIndex = future.get(5, TimeUnit.SECONDS);
                assertTrue(readIndex >= committedFloor - 1,
                        "confirmed read index must reflect the leader's commit state");
            }
            int rounds = follower.readIndexRoundsSent.get();
            assertTrue(rounds >= 1 && rounds <= 3,
                    "a burst issued within one round's flight must share rounds, sent " + rounds);

            // A second, later burst runs on fresh rounds and still resolves.
            assertEquals((int) follower.readIndexFromLeader().get(5, TimeUnit.SECONDS),
                    leader.currentCommitIndex());
        }
    }
}
