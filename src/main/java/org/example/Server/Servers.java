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

    public static void main(String[] args) throws IOException, InterruptedException {

        List<Server> servers = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            int port = 8000 + i; // Ports 8000 to 8004
            Server server = ServerBuilder.forPort(port)
                    .addService(new ServerImpl(i - 1))
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










