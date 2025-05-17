package org.example.Client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.ds.paxos.*;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Test {
    private static final int THREAD_COUNT = 100; // Number of parallel transactions
    private static final int NUM_OF_SERVERS = 9;

    public static void main(String[] args) {
        final int MAX_WRITECONCERN = (NUM_OF_SERVERS / 2) + 1;
        final Random random = new Random();

        while (true) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8003).usePlaintext().build();
            RaftGrpc.RaftBlockingStub stub = RaftGrpc.newBlockingStub(channel);

            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
            CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

            for (int i = 0; i < THREAD_COUNT; i++) {
                executorService.execute(() -> {
                    try {
                        int minConsistency = random.nextInt(MAX_WRITECONCERN) + 1;

                        Transaction parallelTransaction = Transaction.newBuilder()
                                .setId(String.valueOf(System.nanoTime()))
                                .setAmount(1)
                                .setReceiver("Test1")
                                .setSender("Test2")
                                .setMinRequiredConsistency(minConsistency)
                                .setBaseProfit(random.nextInt(30))
                                .setExtraIntermediateProfit(minConsistency == MAX_WRITECONCERN ? 0 : random.nextInt(12))
                                .setExtraProfitMajority(minConsistency == MAX_WRITECONCERN ? 0 : random.nextInt(70))
                                .setTransactionSendTimeInMs(System.currentTimeMillis())
                                .build();

                        ClientMessage message = ClientMessage.newBuilder()
                                .setT(parallelTransaction)
                                .setWriteConcern(minConsistency)
                                .build();

                        stub.sendTransaction(message);

                    } catch (Exception ignored) {
                    } finally {
                        latch.countDown();
                    }
                });
            }

            try {
                latch.await();
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
