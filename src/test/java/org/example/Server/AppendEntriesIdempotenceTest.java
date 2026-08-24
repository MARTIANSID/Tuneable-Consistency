package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.example.raft.AppendEntriesArgument;
import org.example.raft.AppendEntriesResult;
import org.example.raft.LogEntryProto;
import org.junit.jupiter.api.Test;

/**
 * Raft 5.3 follower idempotence: a stream reset can replay an acknowledged
 * prefix. A matching prefix must never be truncated (the 60k TPS sweep
 * caught an unconditional truncation at prevLogIndex+1: a stale empty probe
 * wiped the log, after which the monotonic wait registries let causal reads
 * serve the emptied view); only a genuine (index, term) conflict truncates.
 */
class AppendEntriesIdempotenceTest {

    private static LogEntryProto entry(int index, int term, String key, String value) {
        return LogEntryProto.newBuilder().setLogIndex(index).setTerm(term)
                .setKey(key).setValue(value).setOpId("op" + index).build();
    }

    private static AppendEntriesArgument round(int term, int prevIndex, int prevTerm, int commit,
            LogEntryProto... entries) {
        return AppendEntriesArgument.newBuilder().setLeadersTerm(term).setLeadersId(1)
                .setPrevLogIndex(prevIndex).setPrevLogTerm(prevTerm).setLeadersCommit(commit)
                .addAllEntries(List.of(entries)).build();
    }

    private static AppendEntriesResult apply(ServerImpl node, AppendEntriesArgument args) {
        return node.applyAppendEntriesForTest(args);
    }

    @Test
    void staleAndDuplicateRoundsNeverTruncateAMatchingPrefix() {
        ServerImpl node = new ServerImpl(0, 3);
        try {
            assertTrue(apply(node, round(1, -1, -1, 3,
                    entry(0, 1, "a", "v0"), entry(1, 1, "b", "v1"), entry(2, 1, "a", "v2"),
                    entry(3, 1, "c", "v3"), entry(4, 1, "a", "v4"))).getIsSuccessFull());
            assertEquals(4, node.lastLogIndex());
            assertEquals(3, node.currentCommitIndex());
            assertEquals("v4", node.kv.readLocal("a").value());

            // A stale duplicate round (shorter prefix, older commit) is a no-op.
            assertTrue(apply(node, round(1, -1, -1, 1,
                    entry(0, 1, "a", "v0"), entry(1, 1, "b", "v1"))).getIsSuccessFull());
            assertEquals(4, node.lastLogIndex());
            assertEquals(3, node.currentCommitIndex());
            assertEquals("v4", node.kv.readLocal("a").value());

            // A stale empty probe must not wipe the log.
            assertTrue(apply(node, round(1, -1, -1, 3)).getIsSuccessFull());
            assertEquals(4, node.lastLogIndex());
            assertEquals("v4", node.kv.readLocal("a").value());

            // A partially overlapping round appends only the new tail.
            assertTrue(apply(node, round(1, 2, 1, 3,
                    entry(3, 1, "c", "v3"), entry(4, 1, "a", "v4"), entry(5, 1, "b", "v5"))).getIsSuccessFull());
            assertEquals(5, node.lastLogIndex());
            assertEquals("v5", node.kv.readLocal("b").value());

            // A genuine (index, term) conflict from a newer-term leader
            // truncates the uncommitted suffix only and rebuilds the view.
            assertTrue(apply(node, round(2, 3, 1, 3,
                    entry(4, 2, "d", "w4"))).getIsSuccessFull());
            assertEquals(4, node.lastLogIndex());
            assertEquals("w4", node.kv.readLocal("d").value());
            assertEquals("v2", node.kv.readLocal("a").value());
        } finally {
            node.shutdown();
        }
    }
}
