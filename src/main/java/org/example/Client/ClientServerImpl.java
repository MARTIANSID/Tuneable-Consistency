package org.example.Client;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.ds.paxos.*;
import org.example.Keys.KeyGeneration;
import org.example.Server.ServerImpl;
import org.example.Timer.CustomTimer;

import javax.imageio.IIOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

public class ClientServerImpl extends PbftGrpc.PbftImplBase {
    ConcurrentHashMap<String, List<Reply>> repliesReceived;
    static ConcurrentHashMap<String, CustomTimer> timers;

    ConcurrentHashMap<String, Client> currentClientRequest;

    static ConcurrentHashMap<Integer, Integer> viewNoOfEachClient;

    static ConcurrentHashMap<String, Client> clientRequests;

    ConcurrentHashMap<String, Integer> processedRequests;


    static ConcurrentHashMap<String, CountDownLatch> latchesOfEachClient;

    static ConcurrentHashMap<String,ConcurrentHashMap<String, List<Reply>>> repliesForEachClientAtDifferentTimeStamp;


    static PbftGrpc.PbftStub[] stubs;

    public ClientServerImpl() {

        this.repliesReceived = new ConcurrentHashMap<>();
        this.timers = new ConcurrentHashMap<>();
        this.viewNoOfEachClient = new ConcurrentHashMap<>();
        this.clientRequests = new ConcurrentHashMap<>();
        latchesOfEachClient = new ConcurrentHashMap<>();
        this.processedRequests = new ConcurrentHashMap<>();
        repliesForEachClientAtDifferentTimeStamp = new ConcurrentHashMap<>();
        stubs = new PbftGrpc.PbftStub[13];

        for (int i = 0; i <= 12; i++) {
            if (i == 0) {
                ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9000).usePlaintext().build();
                stubs[i] = PbftGrpc.newStub(channel);
            } else {
                ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8000 + i).usePlaintext().build();
                stubs[i] = PbftGrpc.newStub(channel);
            }
        }

//        for (int i = 1; i <= 3000; i++) {
//            final int id = i;
//            timers.put(i, new CustomTimer(() -> multiCastClientRequest(id), 8, TimeUnit.SECONDS));
//            viewNoOfEachClient.put(i, 0);
//        }
    }

    @Override
    public void sendReplyToClient(Reply reply, StreamObserver<Empty> streamObserver) {

        try {
            Thread.sleep(new Random().nextInt(40));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        int clientId = reply.getClientId();
        String timestamp = reply.getTimestamp();

        String key = clientId + "." + timestamp;


        if (processedRequests.containsKey(key)) {
            streamObserver.onNext(Empty.newBuilder().build());
            streamObserver.onCompleted();
        } else {
            handleReplyLogic(reply, streamObserver);
        }
    }

    public synchronized void handleReplyLogic(Reply reply, StreamObserver<Empty> streamObserver) {
        int clientId = reply.getClientId();
        String timestamp = reply.getTimestamp();

        String key = clientId + "." + timestamp;

        repliesReceived.putIfAbsent(key, new ArrayList<>());
        repliesReceived.get(key).add(reply);

        List<Reply> replies = repliesReceived.get(key);

        int sameReplyCount = 0;

        for (Reply r : replies) {
            if (reply.getTimestamp().equals(r.getTimestamp()) && r.getResult() == r.getResult()) {
                sameReplyCount++;
            }
        }

        System.out.println(sameReplyCount);

        if (sameReplyCount >= 3) {
            processedRequests.put(key, 1);
            repliesForEachClientAtDifferentTimeStamp.putIfAbsent(key, new ConcurrentHashMap<>());
            repliesForEachClientAtDifferentTimeStamp.get(key).putIfAbsent(reply.getTimestamp(), new ArrayList<>());
            repliesForEachClientAtDifferentTimeStamp.get(key).get(reply.getTimestamp()).addAll(replies);
//            viewNoOfEachClient.put(key, Math.max(reply.getViewNo(), viewNoOfEachClient.get(clientId)));
            latchesOfEachClient.get(key).countDown();
            System.out.println("Sufficient Replies Received");
            timers.get(key).stop();
        }

        streamObserver.onNext(Empty.newBuilder().build());
        streamObserver.onCompleted();
    }

    private static int getClusterIndex(int clientId) {
        if (clientId >= 1 && clientId <= 1000) return 1;

        if (clientId >= 1001 && clientId <= 2000) return 2;

        if (clientId >= 2001 && clientId <= 3000) return 3;

        return -1;
    }

    public static void multiCastClientRequest(String clientKey) {
        Client message = clientRequests.get(clientKey);

        System.out.println("Have to multicast");

        Transaction t = message.getT();

        int sender = t.getSenderId(), receiver = t.getReceiverId(), senderCluster = getClusterIndex(sender), receiverCluster = getClusterIndex(receiver), start = 1;

        if(senderCluster == 1) {
            start = 1;
        } else if(senderCluster == 2) {
            start = 5;
        } else {
            start = 9;
        }

//        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8000).usePlaintext().build();
        for (int i = start; i <= (start+3); i++) {
//             channel = ManagedChannelBuilder.forAddress("localhost", 8000 + i).usePlaintext().build();
//
//            PbftGrpc.PbftStub asyncStub = PbftGrpc.newStub(channel);

            stubs[i].clientRequest(message, new StreamObserver<Empty>() {
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
        }
        try {
            Thread.sleep(20);
//            channel.shutdown();
            // resetting the timer
            timers.get(clientKey).reset();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private int getCurrentLeader(Client c) {
        return (viewNoOfEachClient.get(c.getClientId()) % 7);
    }

    static int getCurrentLeaderPortBasedOnClientId(String clientId) {
        return (8000 + (viewNoOfEachClient.get(clientId) % 7));
    }

    static class CsvTransaction {
        int sender;
        int receiver;
        double amount;

        CsvTransaction(int sender, int receiver, double amount) {
            this.sender = sender;
            this.receiver = receiver;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return sender + " " + receiver + " " + amount;
        }

    }

    static String getTimeStamp() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        String timestamp = now.format(formatter);
        return timestamp;
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        PrivateKey clientPrivateKey = KeyGeneration.privateKeys.get(13);
        PublicKey clientPublicKey = KeyGeneration.publicKeys.get(13);

//        HashMap<String, String> client = new HashMap<>();



        Server clientServer = ServerBuilder.forPort(9000)
                .addService(new ClientServerImpl())
                .build()
                .start();

        String inputFilePath = "/Users/sidbansal/Downloads/Lab4_Testset_1.csv";
        List<CsvTransaction> transactions = new ArrayList<>();

        List<List<CsvTransaction>> setOfTransactions = new ArrayList<>();
        List<List<String>> setOfLiveServers = new ArrayList<>();
        List<List<String>> byzantineServers = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(inputFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] columns = line.split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)");


                if (columns.length > 2) {
                    String servers = columns[2].replaceAll("[\\[\\]]", "").trim();
                    servers = servers.replaceAll("^\"|\"$", "").trim();
                    String[] liveServers = servers.split(",\\s*");
                    setOfTransactions.add(new ArrayList<>());
                    setOfLiveServers.add(Arrays.asList(liveServers));
                }

                if (columns.length > 1) {
                    String transactionPart = columns[1].replaceAll("[\\\"()]", "").trim();
                    String[] transactionData = transactionPart.split(", ");
                    int source = Integer.parseInt(transactionData[0]);
                    int destination = Integer.parseInt(transactionData[1]);
                    double weight = Double.parseDouble(transactionData[2]);
                    // Add transaction to the list
                    setOfTransactions.get(setOfTransactions.size() - 1).add(new CsvTransaction(source, destination, weight));
                }

                if (columns.length > 4) {
                    String servers = columns[4].replaceAll("[\\[\\]]", "").trim();
                    servers = servers.replaceAll("^\"|\"$", "").trim();
                    String[] byzantineNodes = servers.split(",\\s*");
                    byzantineServers.add(Arrays.asList(byzantineNodes));
                }


            }
        } catch (IOException e) {
        }


        int setIndex = 0;
        for (int j = 0; j <= 9; j++) {
            if(setIndex > 5) {
                System.out.println("Transactions sets done!");
                return;
            }
            List<CsvTransaction> csvTransactions  = setOfTransactions.get(setIndex);
            Scanner sys = new Scanner(System.in);
            System.out.println("Process Set: " + (setIndex +1));

            boolean isProcess = sys.nextBoolean();

            if(isProcess == false) return;

//            HashMap<String, List<CsvTransaction>> transactionsPerClient = new HashMap<>();

//            for (CsvTransaction t : csvTransactions) {
//
//                String sender = client.get(t.sender), receiver = client.get(t.receiver);
//                double amount = t.amount;
//                transactionsPerClient.putIfAbsent(sender, new ArrayList<>());
//                transactionsPerClient.get(sender).add(new CsvTransaction(sender, receiver, amount));
//            }

            List<String> liveServers = setOfLiveServers.get(setIndex);

            List<String> setOfByzantine = byzantineServers.get(setIndex++);


            HashSet<String> hashSetOfLiveServers = new HashSet<>();

            HashSet<String> hashSetofByzantineServers = new HashSet<>();

            for (String server : liveServers) {
                hashSetOfLiveServers.add(server.trim().toUpperCase());
            }

            for (String server : setOfByzantine) {
                hashSetofByzantineServers.add(server.trim().toUpperCase());
            }


            for (int i = 1; i <= 12; i++) {
                String server = "S" + i;
                if (hashSetOfLiveServers.contains(server.trim().toUpperCase())) {
                    // turn on
                    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8000 + (i)).usePlaintext().build();
                    PbftGrpc.PbftBlockingStub blockingStubForSettingAliveStatus = PbftGrpc.newBlockingStub(channel);
                    blockingStubForSettingAliveStatus.setAlive(Alive.newBuilder().setIsAlive(true).build());
                } else {
                    // turn off
                    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8000 + (i)).usePlaintext().build();
                    PbftGrpc.PbftBlockingStub blockingStubForSettingAliveStatus = PbftGrpc.newBlockingStub(channel);
                    blockingStubForSettingAliveStatus.setAlive(Alive.newBuilder().setIsAlive(false).build());
                }
            }

            for (int i = 1; i <= 12; i++) {
                String server = "S" + i;
                if (hashSetofByzantineServers.contains(server.trim().toUpperCase())) {
                    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8000 + (i)).usePlaintext().build();
                    PbftGrpc.PbftBlockingStub blockingStubForSettingByzantineStatus = PbftGrpc.newBlockingStub(channel);
                    blockingStubForSettingByzantineStatus.setByzantine(Byzantine.newBuilder().setIsByzantine(true).build());

                } else {
                    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8000 + (i)).usePlaintext().build();
                    PbftGrpc.PbftBlockingStub blockingStubForSettingByzantineStatus = PbftGrpc.newBlockingStub(channel);
                    blockingStubForSettingByzantineStatus.setByzantine(Byzantine.newBuilder().setIsByzantine(false).build());
                }
            }

            int index = 0;

            for(CsvTransaction cvT : csvTransactions) {
                String timestampForThisTransaction = getTimeStamp();
                String key = 13+"."+timestampForThisTransaction;
                latchesOfEachClient.put(key, new CountDownLatch(1));

                try {
                    Client message = sendClientMessageToServer(13, cvT.sender, cvT.receiver, cvT.amount, timestampForThisTransaction);
                    clientRequests.put(key, message);
                    timers.put(key, new CustomTimer(() -> multiCastClientRequest(key), 8, TimeUnit.SECONDS));
                    timers.get(key).start();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                index++;
            }

//            Set<String> clients = transactionsPerClient.keySet();
//            int cnt = 0;
//            List<ExecutorService> threadServices = new ArrayList<>();
//            for (String c : clients) {
//                ExecutorService executorService = Executors.newSingleThreadExecutor();
//                threadServices.add(executorService);
//                executorService.submit(() -> {
//                    List<CsvTransaction> transactionsOfClient = transactionsPerClient.get(c);
//                    for (CsvTransaction cvT : transactionsOfClient) {
//                        String timestampForThisTransaction = getTimeStamp();
//                        latchesOfEachClient.put(c + "." + timestampForThisTransaction, new CountDownLatch(1));
//                        try {
//                            Client message = sendClientMessageToServer(c, cvT.sender, cvT.receiver, cvT.amount, timestampForThisTransaction);
//                            clientRequests.put(c+"."+timestampForThisTransaction, message);
//                            timers.get(c).reset();
//                            latchesOfEachClient.get(c + "." + timestampForThisTransaction).await();
//                            timers.get(c).stop();
//                            System.out.println("Sufficient Replies Received");
//                        } catch (Exception e) {
//                            throw new RuntimeException(e);
//                        }
//                    }
//                });
//                Thread.sleep(50);
//            }
//
//            for (ExecutorService service : threadServices) {
//                service.shutdown();
//            }
//            for(ExecutorService service : threadServices) {
//                service.awaitTermination(1, TimeUnit.HOURS);
//            }

        }
        clientServer.awaitTermination();

    }

    public static Client sendClientMessageToServer(int clientId, int sender, int receiver, double amount, String timestamp) throws Exception {
        PrivateKey privateKey = KeyGeneration.privateKeys.get(13);
        PublicKey publicKey = KeyGeneration.publicKeys.get(13);

        Client clientMessage = Client.newBuilder()
                .setTimestamp(timestamp)
                .setClientId(clientId)
                .setT(Transaction.newBuilder().setSenderId(sender).setReceiverId(receiver).setAmount(amount).build())
                .build();

        byte[] signatureBytes = signMessage(clientMessage.toBuilder().clearSignature().build().toByteArray(), privateKey);

        // Attach the signature to the Client message
        Client signedClientMessage = clientMessage.toBuilder()
                .setSignature(ByteString.copyFrom(signatureBytes))
                .build();

        int portOfLeader = 1;

        int senderCluster = getClusterIndex(sender);

        if(senderCluster == 1) {
            portOfLeader = 1;
        } else if(senderCluster == 2) {
            portOfLeader = 5;
        } else {
            portOfLeader = 9;
        }
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8000 + portOfLeader).usePlaintext().build();

        PbftGrpc.PbftStub asyncStub = PbftGrpc.newStub(channel);
        asyncStub.clientRequest(signedClientMessage, new StreamObserver<Empty>() {
            @Override
            public void onNext(Empty reply) {

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        });

        Thread.sleep(200);

        return signedClientMessage;
    }

    public static byte[] signMessage(byte[] messageData, PrivateKey privateKey) throws Exception {
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(privateKey);
        signature.update(messageData);
        return signature.sign();
    }

}
