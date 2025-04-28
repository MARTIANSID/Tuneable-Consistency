package org.example.Client;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.ds.paxos.ClientMessage;
import org.ds.paxos.RaftGrpc;
import org.ds.paxos.Transaction;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Test {
    private static final int THREAD_COUNT = 80; // Number of parallel transactions

    public static void main(String[] args) {


//        stub.printLog(Empty.newBuilder().build());

//        System.out.println(stub.sendReadRequest(ReadRequest.newBuilder().setReadConcern(ReadConcern.LINEARIZABLE).setAccName("Test1").build()));


        while (true) {

            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8001).usePlaintext()
                    .build();
            RaftGrpc.RaftBlockingStub stub = RaftGrpc.newBlockingStub(channel);

//         Parallel execution using threads
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
            CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

            for (int i = 0; i < THREAD_COUNT; i++) {
                executorService.execute(() -> {
                    try {
                        Transaction parallelTransaction = Transaction.newBuilder()
                                .setId(String.valueOf(new Random().nextInt(10000)))
                                .setAmount(1)
                                .setReceiver("Test1")
                                .setSender("Test2")
                                .setTransactionSendTimeInMs(System.currentTimeMillis())
                                .build();
                        Random random = new Random();
                        int result = (random.nextInt(2) == 0) ? 1 : 1;
//                    ClientServerImpl.timeTakenForTransactionToBeExecuted.put(parallelTransaction.getId(), System.currentTimeMillis());
                        stub.sendTransaction(ClientMessage.newBuilder().setT(parallelTransaction).setWriteConcern(result).build());
//                    synchronized (System.out) {
//                        System.out.println("This is readConcern:Linearizability -- " +
//                                stub.sendReadRequest(ReadRequest.newBuilder()
//                                        .setReadConcern(ReadConcern.LINEARIZABLE)
//                                        .setAccName("Test1")
//                                        .build()));
//                        System.out.println("This is readConcern:Local -- " +
//                                stub.sendReadRequest(ReadRequest.newBuilder()
//                                stub.sendReadRequest(ReadRequest.newBuilder()
//                          =              .setReadConcern(ReadConcern.LOCAL)
//                                        .setAccName("Test1")
//                                        .build()));
//                    }
//                    System.out.println("Transaction sent: " + parallelTransaction.getId());
                    } catch (Exception e) {
//                    System.err.println("Error sending transaction: " + e.getMessage());
                    } finally {
                        latch.countDown();
                    }
                });
            }

            try {
                latch.await(); // Wait for all threads to complete
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            executorService.shutdown();
            channel.shutdown();
            System.out.println("All transactions sent.");
            try {
                Thread.sleep(1002);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
