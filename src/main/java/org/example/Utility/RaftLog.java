package org.example.Utility;

import org.ds.paxos.Log;
import org.ds.paxos.LogEntryProto;
import org.ds.paxos.TimeStampProto;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RaftLog {
    private final ArrayList<LogEntry> log;
    private final ReentrantReadWriteLock lock;

    private int NUM_OF_SERVERS;

    public RaftLog(int NUM_OF_SERVERS) {
        this.log = new ArrayList<>();
        this.lock = new ReentrantReadWriteLock();
        this.NUM_OF_SERVERS = NUM_OF_SERVERS;
    }

    public List<LogEntry> logEntriesFromIndex(int start, int end) {
        if (start >= log.size()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(log.subList(start, end));
    }

    public void append(LogEntry entry) {
        lock.writeLock().lock();
        try {
            log.add(entry);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int getCurrentReplicationStatus(int index) {
        int count = 0;
        lock.readLock().lock();
        try {
            for(boolean isReplicated : log.get(index).serversThatReplicatedThisEntry) {
                if(isReplicated) {
                count++;
                }
            }
        } finally {
            lock.readLock().unlock();
        }
        return count;
    }

    public void updateWriteConcern(int index, int serverId, ConcurrentHashMap<Integer, ConcurrentLinkedQueue<Long>> ackTransactionTimeStampsForAllWriteConcerns, Object writeConcernThroughputLock) {
        lock.writeLock().lock();
        try {
            if(log.get(index).writeConcern <= (NUM_OF_SERVERS / 2) && log.get(index).writeConcern > 0 && !log.get(index).serversThatReplicatedThisEntry.get(serverId)) {
                log.get(index).writeConcern = log.get(index).writeConcern - 1;
                log.get(index).serversThatReplicatedThisEntry.set(serverId, true);
                int numberOfServersThisEntryGotReplicatedTo = getCurrentReplicationStatus(index);
                synchronized(writeConcernThroughputLock) {
                    ConcurrentLinkedQueue<Long> queue = ackTransactionTimeStampsForAllWriteConcerns.get(numberOfServersThisEntryGotReplicatedTo);
                    Long currentTime = System.currentTimeMillis();
                    while(!queue.isEmpty() && currentTime - queue.peek() >= 5000L) {
                        queue.poll();
                    }
                    queue.add(currentTime);
                }
                
            }
            // incase of majority above if condition will not be satisfied, but we still want to update the tps
            if(log.get(index).writeConcern == (NUM_OF_SERVERS / 2) + 1) {
                log.get(index).serversThatReplicatedThisEntry.set(serverId, true);
                int numberOfServersThisEntryGotReplicatedTo = getCurrentReplicationStatus(index);
                synchronized(writeConcernThroughputLock) {
                    ConcurrentLinkedQueue<Long> queue = ackTransactionTimeStampsForAllWriteConcerns.get(numberOfServersThisEntryGotReplicatedTo);
                    Long currentTime = System.currentTimeMillis();
                    while(!queue.isEmpty() && currentTime - queue.peek() >= 5000L) {
                        queue.poll();
                    }
                    queue.add(currentTime);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
    public LogEntry get(int index) {
        if (index < 0) {
            return new LogEntry(-1, -1, null, -1,new HybridClock.TimeStamp(0L,0L), new ArrayList<>(), 0,"",-1, -1);
        }
        lock.readLock().lock();
        try {
            return log.get(index);
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<LogEntry> getEntries(int start, int end) {
        lock.readLock().lock();
        try {
           return log.subList(start, end + 1);
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

    // we expect a serverId because we want which server has replicated these entries
    public void appendEntries(Log leadersEntries, int serverId) {

        lock.writeLock().lock();

        try {
            List<LogEntryProto> entries = leadersEntries.getLogList();

            for(LogEntryProto entry : entries) {

                // I use new ArrayList here because for some reason the list returned from protobuf is immutable
                log.add(new LogEntry(entry.getLogIndex(), entry.getTerm(), entry.getT(), entry.getWriteConcern(),
                        HybridClock.TimeStamp.convertToTimeStamp(entry.getTimeStamp()),
                        new ArrayList<>(entry.getServersThatReplicatedThisEntryList()), entry.getCopyOfWriteConcern(), entry.getCallbackHost(), entry.getCallbackPort(), entry.getTimeOfArrivalAtLeader())); // Ensure it's mutable

                // here I update the writeConcern as well, but this method does not acquire lock and is private, this when followers add the entries sent from the leader
                updateWriteConcernInternal(log.size() - 1, serverId);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
    private void updateWriteConcernInternal(int index, int serverId) {
        if (log.get(index).writeConcern <= (NUM_OF_SERVERS / 2) && log.get(index).writeConcern > 0 &&
                !log.get(index).serversThatReplicatedThisEntry.get(serverId)) {
            log.get(index).writeConcern = log.get(index).writeConcern - 1;
            log.get(index).serversThatReplicatedThisEntry.set(serverId, true);
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
        lock.readLock().lock();

        try {
            System.out.println(log);
        } finally {
            lock.readLock().unlock();
        }
    }
}
