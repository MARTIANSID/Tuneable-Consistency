package org.example.Client;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.ds.paxos.Ack;
import org.ds.paxos.Empty;
import org.ds.paxos.RaftGrpc;
import org.ds.paxos.Transaction;
import org.example.Server.ServerImpl;
import org.example.Utility.RaftLog;

import javax.imageio.IIOException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ClientServerImpl extends RaftGrpc.RaftImplBase {
    ConcurrentHashMap<String, Boolean> ackReceived;
    ReadWriteLock lock;

    public ClientServerImpl() {
       this.ackReceived = new ConcurrentHashMap<>();
       this.lock = new ReentrantReadWriteLock();
    }

    // no need to acquire lock as it is already thread safe
    @Override
    public void sendAckToClient(Ack ack, StreamObserver<Empty> responseObserver) {
        List<Transaction> transactions = ack.getTList();

        for (Transaction transaction : transactions) {
            ackReceived.put(transaction.getId(), true);
        }
    }
    public static void main(String[] args) throws IOException {

        // starting client server at 9000 port
        Server server = ServerBuilder.forPort(9000)
                .addService(new ClientServerImpl())
                .build()
                .start();
    }
}