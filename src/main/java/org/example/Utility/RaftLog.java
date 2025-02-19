package org.example.Utility;

import org.ds.paxos.Log;
import org.ds.paxos.LogEntryProto;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    public LogEntry get(int index) {
        if (index < 0) {
            return new LogEntry(-1, -1, null);
        }
        lock.readLock().lock();
        try {
            return log.get(index);
        } finally {
            lock.readLock().unlock();
        }
    }

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

    public List<LogEntry> logEntriesFromIndex(int index) {
        lock.readLock().lock();
        try {
            return new ArrayList<>(log.subList(index, log.size()));
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean checkIfPrevLogIndexHasPrevLogTerm(int prevLogIndex, int prevLogTerm) {
        lock.readLock().lock();

        boolean check = false;
        try {
            if (prevLogIndex == -1 || (prevLogIndex < log.size() && log.get(prevLogIndex).term == prevLogTerm)) {
                check = true;
            }

        } finally {
            lock.readLock().unlock();
            return check;
        }
    }

    public void appendEntries(Log leadersEntries) {

        lock.writeLock().lock();

        try {
            List<LogEntryProto> entries = leadersEntries.getLogList();

            for(LogEntryProto entry : entries) {
                log.add(new LogEntry(entry.getLogIndex(), entry.getTerm(), entry.getT()));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            log.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }
    public void printLog() {
        System.out.println(log);
    }
}
