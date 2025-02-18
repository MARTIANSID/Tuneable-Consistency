package org.example.Server;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.ds.paxos.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Set;

public class ServerState {

    public static <List> void main(String[] args) {
        ServerState serverState = new ServerState();
        Scanner scanner = new Scanner(System.in);

        HashMap<String, Integer> map = new HashMap<>();

        HashMap<String,String> clientMapping = new HashMap<>();
        clientMapping.put("C1", "A");
        clientMapping.put("C2", "B");
        clientMapping.put("C3", "C");
        clientMapping.put("C4", "D");
        clientMapping.put("C5", "E");
        clientMapping.put("C6", "F");
        clientMapping.put("C7", "G");
        clientMapping.put("C8", "H");
        clientMapping.put("C9", "I");
        clientMapping.put("C10", "J");
        map.put("S1",8001);
        map.put("S2", 8002);
        map.put("S3", 8003);
        map.put("S4", 8004);
        map.put("S5", 8005);
        map.put("S6", 8006);
        map.put("S7", 8007);
        map.put("S8", 8008);
        map.put("S9", 8009);
        map.put("S10", 8010);
        map.put("S11", 8011);
        map.put("S12", 8012);



        while (true) {
            // Display menu for user
            System.out.println("Choose a function to execute:");
            System.out.println("1. printBalance");
            System.out.println("2. Performance");
            System.out.println("3. printLog");
            System.out.println("4. printStatus");
            System.out.println("5. printDataStore");
            // Get the user's choice
            int choice = scanner.nextInt();
            scanner.nextLine();  // Consume the newline character

            switch (choice) {
                case 1:
                    System.out.println("You selected printBalance. Please enter the Client Id");
                    int id = scanner.nextInt();

                    int portOfLeader = 1, start = 1;

                    int senderCluster = getClusterIndex(id);

                    if(senderCluster == 1) {
                        portOfLeader = 1;
                        start = 1;
                    } else if(senderCluster == 2) {
                        portOfLeader = 5;
                        start = 5;
                    } else {
                        portOfLeader = 9;
                        start = 9;
                    }

                    System.out.println(start);

                    for(int i = start; i <= (start+3);i++) {
                        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8000 + i).usePlaintext().build();
                        PbftGrpc.PbftBlockingStub blockingStubForPrintingDb = PbftGrpc.newBlockingStub(channel);
                        Balance balance = blockingStubForPrintingDb.printBalance(accId.newBuilder().setId(id).build());
                        System.out.println("Server--- " +  ("S"+i) +" Balance--- " + balance);
                    }

//                    for (int i = 0; i < 7; i++) {
//                        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8000 + i)
//                                .usePlaintext()
//                                .build();
//
//                        PbftGrpc.PbftBlockingStub blockingStubForPrintingDb = PbftGrpc.newBlockingStub(channel);
//                        Balances balances = blockingStubForPrintingDb.printDB(Empty.newBuilder().build());
//                        serverBalances.put("S" + (i + 1), balances);
//                    }
//                    Set<String> servers = serverBalances.keySet();
//
//                    for (String server : servers) {
//                        System.out.println("Below are the balances of server: " + server + "------------");
//                        Balances b = serverBalances.get(server);
//                        for(Balance clientBalance : b.getBalancesList()) {
//                           System.out.println(clientMapping.get(clientBalance.getClientId()) +"->" + clientBalance.getAmount());
//                        }
//                    }
                    break;
                case 2:
                    System.out.println("You selected Performance");
                    long totalLatency = 0;
                    int totalTransactionsProcessed = 0;
                    double throughput = 0;
                    for(int i = 0; i < 7; i ++) {
                        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost",8000 + i).usePlaintext().build();

                        PbftGrpc.PbftBlockingStub blockingStubForGettingPerformance = PbftGrpc.newBlockingStub(channel);
                        Performance p  = blockingStubForGettingPerformance.printPerformance(Empty.newBuilder().build());
                        totalLatency += p.getLatency();
                        System.out.println("Latency" +" of "+"S"+(i+1) + " "+p.getLatency() + " Transactions processed: " + p.getTotalTransactionsProcessed());
                        throughput += p.getLatency() == 0 ? p.getTotalTransactionsProcessed() * 1000 : ((p.getTotalTransactionsProcessed() * 1000) / p.getLatency());
                        totalTransactionsProcessed += p.getTotalTransactionsProcessed();
                    }
                    System.out.println(totalTransactionsProcessed);
                    System.out.println("Latency: " + totalLatency + " Milli seconds");
                    System.out.println("Throughput: " + throughput);
                    break;
                case 3:
                    System.out.println("You selected printLog. Please enter the serverId:");
                    String serverId = scanner.nextLine();
                    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost",  map.get(serverId))
                            .usePlaintext()
                            .build();

                    PbftGrpc.PbftBlockingStub blockingStubForPrintingLog = PbftGrpc.newBlockingStub(channel);
                    Logs logs = blockingStubForPrintingLog.printLog(Empty.newBuilder().build());
                    System.out.println("The Log of the server: " + serverId +" is -------" );
                    System.out.println(logs);

                    break;
                case 4:
                    System.out.println("You selected printStatus. Please enter the sequence number:");
                    int sequenceNumber = scanner.nextInt();
                    for (int i = 0; i < 7; i++) {
                        channel = ManagedChannelBuilder.forAddress("localhost", 8000 + i)
                                .usePlaintext()
                                .build();

                        PbftGrpc.PbftBlockingStub blockingStubForPrintingStatus = PbftGrpc.newBlockingStub(channel);
                        StateAtSequenceNumber state = blockingStubForPrintingStatus.printStatus(SequenceNumber.newBuilder().setSequenceNumber(sequenceNumber).build());
                        System.out.println(state);

                    }

                    break;

                case 5:
                    System.out.println("You selected printDataStore enter the serverId");
                    serverId = scanner.nextLine();

                        channel = ManagedChannelBuilder.forAddress("localhost", map.get(serverId))
                                .usePlaintext()
                                .build();
                        PbftGrpc.PbftBlockingStub blockingStubForPrintingView = PbftGrpc.newBlockingStub(channel);
                        DataStore dataStore = blockingStubForPrintingView.printDataStore(Empty.newBuilder().build());
                        System.out.println(dataStore);
                    break;
                default:
                    System.out.println("Invalid choice. Please choose between 1, 2, 3, 4, or 5.");
                    break;
            }
        }

    }
    private static int getClusterIndex(int clientId) {
        if (clientId >= 1 && clientId <= 1000) return 1;

        if (clientId >= 1001 && clientId <= 2000) return 2;

        if (clientId >= 2001 && clientId <= 3000) return 3;

        return -1;
    }

}