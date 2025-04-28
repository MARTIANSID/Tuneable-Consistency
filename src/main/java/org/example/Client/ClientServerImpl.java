package org.example.Client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.ds.paxos.*;
import org.example.Server.ServerImpl;
import org.example.Utility.HybridClock;
import org.example.Utility.RaftLog;

import java.io.IOException;
import java.sql.Time;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ClientServerImpl extends RaftGrpc.RaftImplBase {
    ConcurrentHashMap<String, Boolean> ackReceived;
    ReadWriteLock lock;
    int currentLeader;
    // used for causal consistency
    HybridClock.TimeStamp lastWriteTimeStamp;

    // clock of client
    HybridClock hybridClock;

    private static final int THREAD_COUNT = 50;

    public static ConcurrentHashMap<String, Long> timeTakenForTransactionToBeExecuted;

    int totalTransactions;

    long totalTime = 0;
    public ClientServerImpl() {
       this.ackReceived = new ConcurrentHashMap<>();
       this.lock = new ReentrantReadWriteLock();
       this.currentLeader = 0;
       this.hybridClock = new HybridClock();
       this.timeTakenForTransactionToBeExecuted = new ConcurrentHashMap<>();
       this.totalTransactions = 0;
       this.totalTime = 0;
    }

    // no need to acquire lock as it is already thread safe
    @Override
    public void sendAckToClient(Ack ack, StreamObserver<Empty> responseObserver) {


        // we ack the server that yes the ack has been received, then implement the ack processing logic
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();


        List<AckMessage> ackMessages = ack.getAckMessageList();

        for (AckMessage ackMessage : ackMessages) {
            Transaction t = ackMessage.getT();
            String id = t.getId();
            if(ackReceived.containsKey(id)) continue;
            totalTransactions += 1;
            totalTime += System.currentTimeMillis() - t.getTransactionSendTimeInMs();
            ackReceived.put(id, true);
        }

        System.out.println(ackMessages.size() + "Messages received on the client end");
        System.out.println("Current Throughput --" + (double)((totalTransactions * 1000) / totalTime));
        System.out.println("Total transactions received till now--" + totalTransactions);
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        // starting client server at 9000 port
        Server server = ServerBuilder.forPort(9000)
                .addService(new ClientServerImpl())
                .build()
                .start();

        System.out.println("Client server started on port 9000");

        server.awaitTermination();
    }
}