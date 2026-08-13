package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;

import org.example.Utility.LogEntry;
import org.example.raft.Transaction;
import org.junit.jupiter.api.Test;

class KvStoreTest {

    private static Transaction write(String key, String value) {
        return Transaction.newBuilder().setId(key + "-" + value).setKey(key).setValue(value).build();
    }

    private static Transaction read(String key) {
        return Transaction.newBuilder().setId("read-" + key).setIsReadOnly(true).setKey(key).build();
    }

    @Test
    void localViewLeadsCommittedView() {
        KvStore kv = new KvStore();

        kv.applyLocal(write("k", "v1"), 0);
        assertEquals("v1", kv.readLocal("k").value());
        assertNull(kv.readCommitted("k"), "uncommitted write must not be visible in the committed view");

        kv.applyCommitted(write("k", "v1"), 0);
        assertEquals("v1", kv.readCommitted("k").value());
    }

    @Test
    void versionsCarryTheModifyingLogIndex() {
        KvStore kv = new KvStore();

        kv.applyLocal(write("k", "v1"), 3);
        kv.applyLocal(write("k", "v2"), 7);
        assertEquals(7, kv.readLocal("k").index());

        kv.applyCommitted(write("k", "v1"), 3);
        assertEquals(3, kv.readCommitted("k").index());
    }

    @Test
    void readsDoNotMutateState() {
        KvStore kv = new KvStore();

        kv.applyLocal(write("k", "v1"), 0);
        kv.applyLocal(read("k"), 1);
        kv.applyCommitted(read("k"), 1);

        assertEquals("v1", kv.readLocal("k").value());
        assertEquals(0, kv.readLocal("k").index());
        assertNull(kv.readCommitted("k"));
    }

    @Test
    void rebuildLocalDiscardsTruncatedEntriesAndKeepsSurvivors() {
        KvStore kv = new KvStore();

        // Index 0 is committed; 1 and 2 are local-only.
        kv.applyLocal(write("a", "committed"), 0);
        kv.applyCommitted(write("a", "committed"), 0);
        kv.applyLocal(write("b", "survives"), 1);
        kv.applyLocal(write("a", "truncated"), 2);

        // Truncation at index 2: entry 1 survives, entry 2 is rolled back.
        LogEntry survivor = new LogEntry(1, 1, write("b", "survives"));
        kv.rebuildLocal(List.of(survivor));

        assertEquals("committed", kv.readLocal("a").value(), "rolled-back write must revert to committed state");
        assertEquals("survives", kv.readLocal("b").value(), "surviving uncommitted write must be replayed");
        assertEquals(1, kv.readLocal("b").index());
        assertEquals("committed", kv.readCommitted("a").value(), "committed view is untouched by rollback");
    }
}
