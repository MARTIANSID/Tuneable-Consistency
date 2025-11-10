package org.example.Client;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.ds.paxos.*;
import org.example.Utility.HybridClock;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

public class ClientServerImpl extends RaftGrpc.RaftImplBase {

    // --- Core State ---
    private final ConcurrentHashMap<String, Boolean> ackReceived;
    private final ReadWriteLock lock;
    private final HybridClock hybridClock;
    private HybridClock.TimeStamp lastTimeStamp;
    private final RaftGrpc.RaftStub[] stubs;
    private final int NUM_OF_SERVERS = 5;

    private int currentLeader = 2;
    private int totalTransactions = 0;
    private long totalTime = 0;

    public static final ConcurrentHashMap<String, Long> timeTakenForTransactionToBeExecuted = new ConcurrentHashMap<>();

    // --- New: Latency tracking per ReadConcern ---
    private final ConcurrentHashMap<ReadConcern, List<Long>> latencyPerConcern = new ConcurrentHashMap<>();

    // --- Constructor ---
    public ClientServerImpl() {
        this.ackReceived = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.hybridClock = new HybridClock();
        this.lastTimeStamp = hybridClock.now();
        this.stubs = new RaftGrpc.RaftStub[NUM_OF_SERVERS];
        // initialize lists for each ReadConcern
        for (ReadConcern rc : ReadConcern.values()) {
            latencyPerConcern.put(rc, Collections.synchronizedList(new ArrayList<>()));
        }
    }

    // --- Setup gRPC Stubs ---
    public void setUpStubs() {
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress("localhost", 8000 + (i + 1))
                    .enableRetry()
                    .usePlaintext()
                    .build();
            stubs[i] = RaftGrpc.newStub(channel);
        }
        System.out.println("✅ Client connected to all " + NUM_OF_SERVERS + " Raft servers");
    }

    // --- Handle ACKs from Raft servers ---
    @Override
    public void sendAckToClient(Ack ack, StreamObserver<Empty> responseObserver) {
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();

        for (AckMessage ackMessage : ack.getAckMessageList()) {
            Transaction t = ackMessage.getT();
            String id = t.getId();

            // Only log successful reads and measure latency
            if (t.getIsReadOnly()) {
                long latency = System.currentTimeMillis() - timeTakenForTransactionToBeExecuted.get(id);
                latencyPerConcern.get(ReadConcern.LINEARIZABLE).add(latency);
//                System.out.println("[READ SUCCESS] ID=" + id +
//                        " | Concern=" + "LINEARIZABLE" +
//                        " | Account=" + ackMessage.getAccName() +
//                        " | Balance=" + ackMessage.getBalance() +
//                        " | Leader=" + ackMessage.getCurrentLeader()
//                        + " | Latency=" + latency + "ms"    );
            }

            if (ackReceived.containsKey(id)) continue;

            totalTransactions++;
            totalTime += System.currentTimeMillis() - t.getTransactionSendTimeInMs();
            ackReceived.put(id, true);
            currentLeader = ackMessage.getCurrentLeader();

            // Update clock if timestamp present
            if (ackMessage.hasTimStamp()) {
                HybridClock.TimeStamp timeStamp = HybridClock.TimeStamp.convertToTimeStamp(ackMessage.getTimStamp());
                hybridClock.update(timeStamp);
                if (lastTimeStamp.compareTo(timeStamp) < 0) {
                    lastTimeStamp = timeStamp;
                }
            }
        }

        // --- Compute average latency per ReadConcern ---
        for (ReadConcern rc : latencyPerConcern.keySet()) {
            List<Long> latencies = latencyPerConcern.get(rc);
            if (!latencies.isEmpty()) {
                double avg = latencies.stream().mapToLong(Long::longValue).average().orElse(0);
                System.out.println("[AVG LATENCY] Concern=" + rc + " | AvgLatency=" + avg + "ms");
            }
        }
    }

    // --- Send Read Request (with optional provided ID) ---
    public void sendReadRequest(String accName, ReadConcern readConcern, ReadLevel readLevel, String readId) {
        String requestId = readId != null ? readId : UUID.randomUUID().toString();

        ClientReadRequest request = ClientReadRequest.newBuilder()
                .setAccNameToRead(accName)
                .setReadConcern(readConcern)
                .setReadLevel(readLevel)
                .setTimeStamp(HybridClock.TimeStamp.convertToProto(lastTimeStamp))
                .setId(requestId)
                .build();

        CountDownLatch latch = new CountDownLatch(1);
        RaftGrpc.RaftStub leaderStub;

        lock.readLock().lock();
        try {
            if(readConcern == ReadConcern.LINEARIZABLE)
                leaderStub = stubs[currentLeader];
            else
                leaderStub = stubs[new Random().nextInt(NUM_OF_SERVERS)];
        } finally {
            lock.readLock().unlock();
        }

        long sendTime = System.currentTimeMillis();

        leaderStub.sendReadRequest(request, new StreamObserver<>() {
            @Override
            public void onNext(Ack ack) {
                for (AckMessage msg : ack.getAckMessageList()) {
                    if (!msg.getFailure() && !msg.getResultNotReady()) {
                        long latency = System.currentTimeMillis() - sendTime;
                        latencyPerConcern.get(readConcern).add(latency);
//                        System.out.println("[READ OK] ID=" + requestId +
//                                " | Concern=" + readConcern +
//                                " | Account=" + msg.getAccName() +
//                                " | Balance=" + msg.getBalance() +
//                                " | Latency=" + latency + "ms");
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        });

        try {
            latch.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // --- Overload for standalone reads ---
    public void sendReadRequest(String accName, ReadConcern readConcern, ReadLevel readLevel) {
        sendReadRequest(accName, readConcern, readLevel, null);
    }

    // --- Main Method: Parallel Periodic Reads with unique IDs ---
    public static void main(String[] args) throws IOException, InterruptedException {
        ClientServerImpl clientServer = new ClientServerImpl();

        Server server = ServerBuilder.forPort(9000)
                .addService(clientServer)
                .build()
                .start();

        System.out.println("🚀 Client server started on port 9000");
        clientServer.setUpStubs();

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        String accountName = "AcctA";

        // Schedule parallel read groups every 100ms
        // Schedule a recurring task
        scheduler.scheduleAtFixedRate(() -> {
            System.out.println("\n=== 🧩 NEW READ GROUP ===");

            int transactionCount = 50; // number of concurrent transactions
            ExecutorService exec = Executors.newFixedThreadPool(transactionCount);

            String id = UUID.randomUUID().toString();
            timeTakenForTransactionToBeExecuted.put(id, System.currentTimeMillis());

            // Submit 30 read requests in parallel
            for (int i = 0; i < transactionCount; i++) {
                int index = i; // capture loop variable
                exec.submit(() -> {
                    ReadConcern concern;
                    ReadLevel level;

                    if (index % 3 == 0) {
                        concern = ReadConcern.EVENTUAL;
                        level = ReadLevel.LOCAL;
                    } else if (index % 3 == 1) {
                        concern = ReadConcern.CAUSAL;
                        level = ReadLevel.MAJORITY;
                    } else {
                        concern = ReadConcern.LINEARIZABLE;
                        level = ReadLevel.MAJORITY;
                    }

                    clientServer.sendReadRequest(accountName, concern, level, id);
                });
            }

            exec.shutdown();

        }, 20, 50, TimeUnit.MILLISECONDS); // initial delay = 20ms, run every 1 second
        System.out.println("⏱️ Parallel read groups scheduled every 100ms with unique IDs");
        server.awaitTermination();
    }
}
