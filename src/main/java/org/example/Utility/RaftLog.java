package org.example.Utility;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RaftLog {
    private final ArrayList<Log> log;
    private final ReentrantReadWriteLock lock;

    public RaftLog() {
        this.log = new ArrayList<>();
        this.lock = new ReentrantReadWriteLock();
    }

    public void append(Log entry) {
        lock.writeLock().lock();
        try {
            log.add(entry);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Log get(int index) {
        if(index < 0) {
            return new Log(-1,-1, null);
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
    public Log getLastLogEntry() {
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


    public void clear() {
        lock.writeLock().lock();
        try {
            log.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
