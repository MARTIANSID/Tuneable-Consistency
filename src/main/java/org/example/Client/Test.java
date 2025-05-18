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
    private static final int THREAD_COUNT = 12;    // Number of parallel transactions
    private static final int NUM_OF_SERVERS = 9;
    private static final int MAX_WRITECONCERN = (NUM_OF_SERVERS / 2) + 1;

    private static final double[] WC_WEIGHTS = new double[] {
        0.6,  // weight for write-concern = 1
        0.1,  // weight for write-concern = 2
        0.1,  // weight for write-concern = 3
        0.1,  // weight for write-concern = 4
        0.1   // weight for write-concern = 5 (MAX_WRITECONCERN)
    };

    private static final Random random = new Random();

 
    private static int pickWeightedWriteConcern() {
        // 1) compute sum of weights
        double total = 0;
        for (double w : WC_WEIGHTS) {
            total += w;
        }

        // 2) pick a random point in [0, total)
        double r = random.nextDouble() * total;

        // 3) walk the cumulative distribution
        double cum = 0;
        for (int i = 0; i < WC_WEIGHTS.length; i++) {
            cum += WC_WEIGHTS[i];
            if (r < cum) {
                // +1 because levels go from 1..MAX_WRITECONCERN, not 0..
                return i + 1;
            }
        }

        // fallback (shouldn't happen unless rounding error)
        return MAX_WRITECONCERN;
    }

    public static void main(String[] args) {
        while (true) {
            ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 8003)
                .usePlaintext()
                .build();
            RaftGrpc.RaftBlockingStub stub = RaftGrpc.newBlockingStub(channel);

            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
            CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

            for (int i = 0; i < THREAD_COUNT; i++) {
                executorService.execute(() -> {
                    try {
                        int minConsistency = pickWeightedWriteConcern();

                        Transaction tx = Transaction.newBuilder()
                            .setId(String.valueOf(System.nanoTime()))
                            .setAmount(1)
                            .setReceiver("Test1")
                            .setSender("Test2")
                            .setMinRequiredConsistency(minConsistency)
                            .setBaseProfit(1)
                            .setExtraIntermediateProfit(
                                minConsistency == MAX_WRITECONCERN ? 1 : 1)
                            .setExtraProfitMajority(
                                minConsistency == MAX_WRITECONCERN ? 1 : 1)
                            .setTransactionSendTimeInMs(System.currentTimeMillis())
                            .build();

                        ClientMessage message = ClientMessage.newBuilder()
                            .setT(tx)
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
                Thread.currentThread().interrupt();
            }

            executorService.shutdown();
            channel.shutdown();
            System.out.println("All transactions sent.");
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
