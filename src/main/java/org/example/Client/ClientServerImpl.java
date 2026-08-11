package org.example.Client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.ds.paxos.Ack;
import org.ds.paxos.AckMessage;
import org.ds.paxos.Empty;
import org.ds.paxos.RaftGrpc;
import org.ds.paxos.ReadConcern;
import org.ds.paxos.TimeStampProto;
import org.ds.paxos.Transaction;
import org.example.Utility.HybridClock;

import io.grpc.stub.StreamObserver;

/**
 * Client-side callback endpoint. Servers push commit ACKs here via the
 * sendAckToClient RPC; this class records end-to-end latency and keeps the
 * hybrid-clock timestamp that the injector attaches to new transactions.
 */
public class ClientServerImpl extends RaftGrpc.RaftImplBase {

    // --- Core State ---
    private final ConcurrentHashMap<String, Boolean> ackReceived;
    private final ReadWriteLock lock;
    private final HybridClock hybridClock;
    private HybridClock.TimeStamp lastTimeStamp;

    public static final ConcurrentHashMap<String, Long> timeTakenForTransactionToBeExecuted = new ConcurrentHashMap<>();

    // --- Latency tracking per ReadConcern ---
    private final ConcurrentHashMap<ReadConcern, List<Long>> latencyPerConcern = new ConcurrentHashMap<>();

    // --- Constructor ---
    public ClientServerImpl() {
        this.ackReceived = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.hybridClock = new HybridClock();
        this.lastTimeStamp = hybridClock.now();
        // initialize lists for each ReadConcern
        for (ReadConcern rc : ReadConcern.values()) {
            latencyPerConcern.put(rc, Collections.synchronizedList(new ArrayList<>()));
        }
    }

    // --- Handle ACKs from Raft servers ---
    @Override
    public void sendAckToClient(Ack ack, StreamObserver<Empty> responseObserver) {
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();

        for (AckMessage ackMessage : ack.getAckMessageList()) {
            Transaction t = ackMessage.getT();
            String id = t.getId();

            // Only log successful reads and measure latency
            if (t.getIsReadOnly()) {
                Long start = timeTakenForTransactionToBeExecuted.get(id);
                if (start != null) {
                    long latency = System.currentTimeMillis() - start;
                    latencyPerConcern.get(ReadConcern.LINEARIZABLE).add(latency);
                }
            }

            if (ackReceived.containsKey(id)) continue;

            ackReceived.put(id, true);

            // Update clock if timestamp present
            if (ackMessage.hasTimStamp()) {
                HybridClock.TimeStamp timeStamp = HybridClock.TimeStamp.convertToTimeStamp(ackMessage.getTimStamp());
                hybridClock.update(timeStamp);
                if (lastTimeStamp.compareTo(timeStamp) < 0) {
                    lastTimeStamp = timeStamp;
                }
            }
        }

        // --- Compute average latency per ReadConcern ---
        for (ReadConcern rc : latencyPerConcern.keySet()) {
            List<Long> latencies = latencyPerConcern.get(rc);
            if (!latencies.isEmpty()) {
                double avg = latencies.stream().mapToLong(Long::longValue).average().orElse(0);
                System.out.println("[AVG LATENCY] Concern=" + rc + " | AvgLatency=" + avg + "ms");
            }
        }
    }

    // --- Expose latest ACK-updated timestamp for external injectors ---
    public TimeStampProto getLastTimeStampProto() {
        lock.readLock().lock();
        try {
            return HybridClock.TimeStamp.convertToProto(lastTimeStamp);
        } finally {
            lock.readLock().unlock();
        }
    }
}
