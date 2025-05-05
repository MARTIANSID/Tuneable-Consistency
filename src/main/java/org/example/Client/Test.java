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
    private static final int THREAD_COUNT = 1; // Number of parallel transactions

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

            Random random = new Random();
            String[] appTypes = {
                "post" ,            // 40% posts (must w:majority → cost 1)
                "comment",  // 45% comments (w:1 → can upgrade)
                "like","like"  ,"like"                     // 15% likes
              };
                  

            for (int i = 0; i < THREAD_COUNT; i++) {
                executorService.execute(() -> {
                    try {

                        String appType = appTypes[random.nextInt(appTypes.length)];
                        int writeConcern;

                        // Set minimum consistency based on app type
                        if (appType.equals("post")) {
                            writeConcern = 2; // assuming 2 = majority
                        } else {
                            writeConcern = 1; // w:1
                        }
                        Transaction parallelTransaction = Transaction.newBuilder()
                                .setId(String.valueOf(new Random().nextInt(10000)))
                                .setAmount(1)
                                .setReceiver("Test1")
                                .setSender("Test2")
                                .setTransactionSendTimeInMs(System.currentTimeMillis())
                                .setAppType(appType)  // <-- important (proto must support appType string)
                                .build();
                        // int result = (random.nextInt(2) == 0) ? 1 : ;
                        // stub.sendTransaction(ClientMessage.newBuilder().setT(parallelTransaction).setWriteConcern(result).build());
                        stub.sendTransaction(ClientMessage.newBuilder()
                        .setT(parallelTransaction)
                        .setWriteConcern(writeConcern)
                        .build());
                    } catch (Exception e) {
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
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
