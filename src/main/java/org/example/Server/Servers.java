package org.example.Server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.ds.paxos.ClientMessage;
import org.ds.paxos.Transaction;
import org.ds.paxos.TimeStampProto;

public class Servers{

    // set the number of servers from here
    public static final int NUM_OF_SERVERS =  3;
    
    // Static reference to TransactionInjector for testing
    public static TransactionInjector injector;

    // Configuration for transaction injection
    private static final int TRANSACTIONS_PER_SECOND = 20000;  // Target TPS - 40k
    private static final int BATCH_SIZE = 2000;                 // Transactions per batch (larger batches for high TPS)
    private static final int INJECTION_DURATION_SECONDS = 60;  // How long to inject

    public static void main(String[] args) throws IOException{
        List<Server> servers = new ArrayList<>();
        List<ServerImpl> serversImpl = new ArrayList<>();
        for (int i = 1; i <= NUM_OF_SERVERS; i++) {
            int port = 8000 + i; // Ports 8000 to 8004
            ServerImpl serverImpl = new ServerImpl(i - 1, NUM_OF_SERVERS);
            Server server = ServerBuilder.forPort(port)
                    .addService(serverImpl)
//                    .executor(Executors.newFixedThreadPool(1)) // limit to 2 threads
                    .build()
                    .start();
            System.out.println("Server" + (i - 1) + " started on port " + port);
            serverImpl.setUpStubs();
            servers.add(server);
            serversImpl.add(serverImpl);
        }
        
        // Initialize the TransactionInjector
        injector = new TransactionInjector(serversImpl);
        
        // Start transaction injection in a separate thread
        startTransactionInjection();
        
        for (int i = 0; i < servers.size(); i++) {
            final int index = i;
            Server server = servers.get(i);
                try {
                    server.awaitTermination();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
        }
    }

    /**
     * Starts injecting transactions at a controlled rate using a single scheduled thread.
     * No need for multiple threads - uses a scheduler to inject batches periodically.
     */
    private static void startTransactionInjection() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        
        // Calculate injection interval
        // e.g., 1000 TPS with batch size 100 = inject every 100ms
        int batchesPerSecond = TRANSACTIONS_PER_SECOND / BATCH_SIZE;
        long intervalMs = 1000 / batchesPerSecond;
        
        System.out.println("📊 Transaction Injection Config:");
        System.out.println("   Target TPS: " + TRANSACTIONS_PER_SECOND);
        System.out.println("   Batch Size: " + BATCH_SIZE);
        System.out.println("   Interval: " + intervalMs + "ms");
        System.out.println("   Duration: " + INJECTION_DURATION_SECONDS + "s");
        
        // Wait for leader election before injecting
        new Thread(() -> {
            System.out.println("⏳ Waiting for leader election...");
            ServerImpl leader = injector.waitForLeader(10000);
            if (leader == null) {
                System.err.println("❌ No leader elected, aborting injection");
                return;
            }
            
            System.out.println("🚀 Starting transaction injection...");
            final long startTime = System.currentTimeMillis();
            final long endTime = startTime + (INJECTION_DURATION_SECONDS * 1000L);
            
            scheduler.scheduleAtFixedRate(() -> {
                if (System.currentTimeMillis() > endTime) {
                    System.out.println("✅ Injection complete!");
                    injector.printAllServersStatus();
                    scheduler.shutdown();
                    return;
                }
                
                // Generate a batch of transactions
                List<ClientMessage> batch = generateTransactionBatch(BATCH_SIZE);
                
                // Inject directly into leader's batch queue (no gRPC overhead)
                injector.injectIntoLeader(batch);
                
            }, 0, intervalMs, TimeUnit.MILLISECONDS);
            
        }).start();
    }

    /**
     * Generate a batch of test transactions efficiently.
     * All transactions are created in memory without network calls.
     */
    private static List<ClientMessage> generateTransactionBatch(int size) {
        List<ClientMessage> batch = new ArrayList<>(size);
        long now = System.currentTimeMillis();
        
        for (int i = 0; i < size; i++) {
            String txId = UUID.randomUUID().toString();
            String sender = "user" + (i % 100);      // 100 different senders
            String receiver = "user" + ((i + 50) % 100);  // Different receivers
            double amount = 1.0 + (i % 10);          // Amounts 1-10
            
            Transaction transaction = Transaction.newBuilder()
                    .setId(txId)
                    .setSender(sender)
                    .setReceiver(receiver)
                    .setAmount(amount)
                    .setTransactionSendTimeInMs(now)
                    .setMinRequiredConsistency(1)    // Minimum consistency
                    .setBaseProfit(1.0)
                    .setExtraProfitMajority(0.5)
                    .setExtraIntermediateProfit(0.25)
                    .setWriteConcern(2)              // Default write concern
                    .setIsReadOnly(false)
                    .build();
            
            ClientMessage message = ClientMessage.newBuilder()
                    .setT(transaction)
                    .setWriteConcern(2)
                    .setTimeStamp(TimeStampProto.newBuilder()
                            .setP(now)
                            .setL(i)
                            .build())
                    .setCallbackHost("localhost")
                    .setCallbackPort(9000)           // Client callback port
                    .build();
            
            batch.add(message);
        }
        
        return batch;
    }
}








