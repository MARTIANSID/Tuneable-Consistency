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
import java.util.concurrent.Executors;

public class Servers {

    public static final int NUM_OF_SERVERS = 3;

    public static void main(String[] args) throws IOException {
        // if (args.length != 1) {
        //     System.err.println("Usage: java org.example.Server.Servers <serverId>");
        //     System.exit(1);
        // }

        int serverId = 0;
        int port = 8000 + (0);

        ServerImpl serverImpl = new ServerImpl(serverId, NUM_OF_SERVERS);
        Server server = ServerBuilder.forPort(port)
                .addService(serverImpl)
                .build()
                .start();

        System.out.println("Server " + serverId + " started on port " + port);
        serverImpl.setUpStubs();

        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }
}









