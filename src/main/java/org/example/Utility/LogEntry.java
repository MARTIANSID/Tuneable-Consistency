package org.example.Utility;

/**
 * One replicated KV write. Reads never enter the log (linearizable reads use
 * ReadIndex), so entries carry only the write payload plus Raft bookkeeping.
 * Write-concern acknowledgment tracking lives in the leader's wait
 * registries, not in the entry.
 */
public final class LogEntry {
    public final int index;
    public final int term;
    public final String key;
    public final String value;
    public final String opId;

    public LogEntry(int index, int term, String key, String value, String opId) {
        this.index = index;
        this.term = term;
        this.key = key;
        this.value = value;
        this.opId = opId;
    }

    @Override
    public String toString() {
        return "LogEntry{index=" + index + ", term=" + term + ", key=" + key + "}";
    }
}
