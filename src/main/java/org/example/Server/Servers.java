package org.example.Server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public class Servers{

    // set the number of servers from here
    public static final int NUM_OF_SERVERS =  3;

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
}









