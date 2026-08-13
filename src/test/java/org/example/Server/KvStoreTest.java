package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;

import org.example.Utility.LogEntry;
import org.junit.jupiter.api.Test;

class KvStoreTest {

    private static LogEntry write(int index, String key, String value) {
        return new LogEntry(index, 1, key, value, key + "-" + value);
    }

    @Test
    void localViewLeadsCommittedView() {
        KvStore kv = new KvStore();

        kv.applyLocal(write(0, "k", "v1"));
        assertEquals("v1", kv.readLocal("k").value());
        assertNull(kv.readCommitted("k"), "uncommitted write must not be visible in the committed view");

        kv.applyCommitted(write(0, "k", "v1"));
        assertEquals("v1", kv.readCommitted("k").value());
    }

    @Test
    void versionsCarryTheModifyingLogIndex() {
        KvStore kv = new KvStore();

        kv.applyLocal(write(3, "k", "v1"));
        kv.applyLocal(write(7, "k", "v2"));
        assertEquals(7, kv.readLocal("k").index());

        kv.applyCommitted(write(3, "k", "v1"));
        assertEquals(3, kv.readCommitted("k").index());
    }

    @Test
    void rebuildLocalDiscardsTruncatedEntriesAndKeepsSurvivors() {
        KvStore kv = new KvStore();

        // Index 0 is committed; 1 and 2 are local-only.
        kv.applyLocal(write(0, "a", "committed"));
        kv.applyCommitted(write(0, "a", "committed"));
        kv.applyLocal(write(1, "b", "survives"));
        kv.applyLocal(write(2, "a", "truncated"));

        // Truncation at index 2: entry 1 survives, entry 2 is rolled back.
        kv.rebuildLocal(List.of(write(1, "b", "survives")));

        assertEquals("committed", kv.readLocal("a").value(), "rolled-back write must revert to committed state");
        assertEquals("survives", kv.readLocal("b").value(), "surviving uncommitted write must be replayed");
        assertEquals(1, kv.readLocal("b").index());
        assertEquals("committed", kv.readCommitted("a").value(), "committed view is untouched by rollback");
    }
}
