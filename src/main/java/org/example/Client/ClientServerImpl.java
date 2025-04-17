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
import org.ds.paxos.RaftGrpc;
import org.ds.paxos.AckMessage;
import org.ds.paxos.Ack;
import org.ds.paxos.Empty;


import java.io.IOException;
import java.sql.Time;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
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
    public ClientServerImpl() {
       this.ackReceived = new ConcurrentHashMap<>();
       this.lock = new ReentrantReadWriteLock();
       this.currentLeader = 0;
       this.hybridClock = new HybridClock();
    }

    // no need to acquire lock as it is already thread safe
    @Override
    public void sendAckToClient(Ack ack, StreamObserver<Empty> responseObserver) {
        List<AckMessage> ackMessages = ack.getAckMessageList();

        for (AckMessage ackMessage : ackMessages) {
            ackReceived.put(ackMessage.getT().getId(), true);
        }

        System.out.println(ackMessages.size() + "Messages received on the client end");
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        // starting client server at 9000 port
        Server server = ServerBuilder.forPort(9000)
                .addService(new ClientServerImpl())
                .build()
                .start();


        server.awaitTermination();
    }
}