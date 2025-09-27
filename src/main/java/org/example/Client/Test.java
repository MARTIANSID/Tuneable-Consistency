package org.example.Client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.ds.paxos.*;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Test {
    private static final int THREAD_COUNT = 12; // Number of parallel transactions
    private static final int NUM_OF_SERVERS = 9;

    private static int pickWeightedWriteConcern() {
        // 1) compute sum of weights
        double total = 0;
        for (double w : WC_WEIGHTS) {
            total += w;
        }

        final Random random = new Random();
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

    static private int baseProfit(int minConsistency) {
        return minConsistency;
    }

    public static final double[] WC_WEIGHTS = new double[] {
            0.5,  // weight for write-concern = 1
            0.1,  // weight for write-concern = 2
            0.1,  // weight for write-concern = 3
            0.1,  // weight for write-concern = 4
            0.2   // weight for write-concern = 5 (MAX_WRITECONCERN)
    };

    static final int MAX_WRITECONCERN = (NUM_OF_SERVERS / 2) + 1;
    public static void main(String[] args) {
        final Random random = new Random();

        while (true) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8003).usePlaintext().build();
            RaftGrpc.RaftBlockingStub stub = RaftGrpc.newBlockingStub(channel);

            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
            CountDownLatch latch = new CountDownLatch(THREAD_COUNT);


            for (int i = 0; i < THREAD_COUNT; i++) {
                executorService.execute(() -> {
                    try {
                        int minConsistency = pickWeightedWriteConcern();

                        Transaction parallelTransaction = Transaction.newBuilder()
                                .setId(String.valueOf(System.nanoTime()))
                                .setAmount(1)
                                .setReceiver("Test1")
                                .setSender("Test2")
                                .setMinRequiredConsistency(minConsistency)
                                .setBaseProfit(baseProfit(minConsistency))
                                .setExtraIntermediateProfit(minConsistency == MAX_WRITECONCERN ? 0 : 0.5)
                                .setExtraProfitMajority(minConsistency == MAX_WRITECONCERN ? 0 : 0.5)
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
            System.out.println("All transactions sent.");
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
