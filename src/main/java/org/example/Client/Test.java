package org.example.Client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.ds.paxos.*;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.example.Client.ClientServerImpl.*;

public class Test {
    private static final int THREAD_COUNT = 230; // Number of parallel transactions

    public static void main(String[] args) {

//        System.out.println(stub.sendReadRequest(ReadRequest.newBuilder().setReadConcern(ReadConcern.LINEARIZABLE).setAccName("Test1").build()));

        while (true) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8003).usePlaintext()
                    .build();
            RaftGrpc.RaftBlockingStub stub = RaftGrpc.newBlockingStub(channel);

            stub.printLog(Empty.newBuilder().build());
            if(true)
            break;

//         Parallel execution using threads
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
            CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

            for (int i = 0; i < THREAD_COUNT; i++) {
                executorService.execute(() -> {
                    try {
                        Random random = new Random();
                        int minConsistency = (random.nextInt(2) + 1);
                        Transaction parallelTransaction = Transaction.newBuilder()
                                .setId(String.valueOf(new Random().nextInt(10000)))
                                .setAmount(1)
                                .setReceiver("Test1")
                                .setSender("Test2")
                                .setMinRequiredConsistency(minConsistency)
                                .setBaseProfit(random.nextInt(30))
                                .setExtraProfitMajority( minConsistency == 2 ? 0 : random.nextInt(70))
                                .setTransactionSendTimeInMs(System.currentTimeMillis())
                                .build();
                        int result = (random.nextInt(2) == 0) ? 1 : 2;
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
