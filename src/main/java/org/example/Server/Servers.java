package org.example.Server;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class Servers{

    // set the number of servers from here
    public static final int NUM_OF_SERVERS =  9;

    public static void main(String[] args) throws IOException, InterruptedException {
        List<Server> servers = new ArrayList<>();
        for (int i = 1; i <= NUM_OF_SERVERS; i++) {
            int port = 8000 + i; // Ports 8000 to 8004
            Server server = ServerBuilder.forPort(port)
                    .addService(new ServerImpl(i - 1, NUM_OF_SERVERS))
                    .build()
                    .start();
            System.out.println("Server" + (i + 1) + " started on port " + port);
            servers.add(server);
        }
        for (Server server : servers) {
            server.awaitTermination();
        }

    }
}

//nextIndex = [4,4,4]
//        matchIndex = [f3, 4, f2, ]
//f1 - [1,2, 3, 4]
//f2 - [1,2,3,4]
//f3 - [1,2, 3 , 4]
////l1  - [1,2,3,4]
//commitindex  = 2










