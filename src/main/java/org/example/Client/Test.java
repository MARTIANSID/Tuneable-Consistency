package org.example.Client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.ds.paxos.ClientMessage;
import org.ds.paxos.Empty;
import org.ds.paxos.RaftGrpc;
import org.ds.paxos.Transaction;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Test {
    private static final int THREAD_COUNT = 20; // Number of parallel transactions

    public static void main(String[] args) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8005)
                .usePlaintext()
                .build();
        RaftGrpc.RaftBlockingStub stub = RaftGrpc.newBlockingStub(channel);
        RaftGrpc.RaftStub stub2 = RaftGrpc.newStub(channel);

        stub.printLog(Empty.newBuilder().build());


        Transaction t = Transaction.newBuilder()
                .setId(new Random().nextInt(10000) + "")
                .setAmount(new Random().nextInt(200))
                .setReceiver("Test1")
                .setSender("Test2")
                .build();

        // Keeping the existing gRPC call
//        stub.sendTransaction(ClientMessage.newBuilder().setT(t).build());

//        try {
//            Thread.sleep(300);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//
//        // Parallel execution using threads
//        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
//        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
//
//        for (int i = 0; i < THREAD_COUNT; i++) {
//            executorService.execute(() -> {
//                try {
//                    Transaction parallelTransaction = Transaction.newBuilder()
//                            .setId(String.valueOf(new Random().nextInt(10000)))
//                            .setAmount(new Random().nextInt(200))
//                            .setReceiver("Test1")
//                            .setSender("Test2")
//                            .build();
//
//                    stub.sendTransaction(ClientMessage.newBuilder().setT(parallelTransaction).build());
//                    System.out.println("Transaction sent: " + parallelTransaction.getId());
//                } catch (Exception e) {
//                    System.err.println("Error sending transaction: " + e.getMessage());
//                } finally {
//                    latch.countDown();
//                }
//            });
//        }
//
//        try {
//            latch.await(); // Wait for all threads to complete
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//
//        executorService.shutdown();
        channel.shutdown();
        System.out.println("All transactions sent.");
    }
}
