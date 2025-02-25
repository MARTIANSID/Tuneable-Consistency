package org.example.Client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.ds.paxos.ClientMessage;
import org.ds.paxos.Empty;
import org.ds.paxos.RaftGrpc;
import org.ds.paxos.Transaction;

import java.util.Random;

public class Test {
    public static void main(String[] args) {

       ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8005).usePlaintext().build();
        RaftGrpc.RaftStub stub = RaftGrpc.newStub(channel);
        Transaction t = Transaction.newBuilder().setAmount(new Random().nextInt(200)).setReceiver("Test1").setSender("Test2").build();
//
        stub.printLog(Empty.newBuilder().build(), new StreamObserver<Empty>() {
            @Override
            public void onNext(Empty empty) {

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        });
//        stub.sendTransaction(ClientMessage.newBuilder().setT(t).build(), new StreamObserver<Empty>() { @Override public void onNext(Empty empty) {
//
//        }
//
//            @Override
//            public void onError(Throwable throwable) {
//
//            }
//
//            @Override
//            public void onCompleted() {
//
//            }
//        });

        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
