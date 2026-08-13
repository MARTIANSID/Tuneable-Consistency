package org.example.Utility;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.example.raft.LogEntryProto;

/**
 * The Raft log: an in-memory, 0-indexed list of KV write entries. Thread safe
 * via an internal read-write lock; the owning ServerImpl additionally orders
 * append/truncate against its own consensus lock.
 */
public class RaftLog {
    private final ArrayList<LogEntry> log;
    private final ReentrantReadWriteLock lock;

    public RaftLog() {
        this.log = new ArrayList<>();
        this.lock = new ReentrantReadWriteLock();
    }

    public void append(LogEntry entry) {
        lock.writeLock().lock();
        try {
            log.add(entry);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Append entries received from the leader (already consistency-checked); returns them. */
    public List<LogEntry> appendProtoEntries(List<LogEntryProto> entries) {
        lock.writeLock().lock();
        try {
            List<LogEntry> appended = new ArrayList<>(entries.size());
            for (LogEntryProto entry : entries) {
                LogEntry logEntry = new LogEntry(entry.getLogIndex(), entry.getTerm(), entry.getKey(),
                        entry.getValue(), entry.getOpId());
                log.add(logEntry);
                appended.add(logEntry);
            }
            return appended;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public LogEntry get(int index) {
        lock.readLock().lock();
        try {
            return log.get(index);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Copy of entries in [start, end); clamped to the log size. */
    public List<LogEntry> entriesInRange(int start, int end) {
        lock.readLock().lock();
        try {
            int from = Math.max(0, start);
            int to = Math.min(end, log.size());
            if (from >= to) {
                return Collections.emptyList();
            }
            return new ArrayList<>(log.subList(from, to));
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Remove entries from index (inclusive) to the end. */
    public void truncateAfter(int index) {
        lock.writeLock().lock();
        try {
            log.subList(index, log.size()).clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public LogEntry getLastLogEntry() {
        lock.readLock().lock();
        try {
            return log.isEmpty() ? null : log.get(log.size() - 1);
        } finally {
            lock.readLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return log.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean isEmpty() {
        lock.readLock().lock();
        try {
            return log.isEmpty();
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean checkIfPrevLogIndexHasPrevLogTerm(int prevLogIndex, int prevLogTerm) {
        lock.readLock().lock();
        try {
            return prevLogIndex == -1 || (prevLogIndex < log.size() && log.get(prevLogIndex).term == prevLogTerm);
        } finally {
            lock.readLock().unlock();
        }
    }
}
