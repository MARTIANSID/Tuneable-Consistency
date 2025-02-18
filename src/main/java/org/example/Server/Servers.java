package org.example.Server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.example.Keys.KeyGeneration;

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

        Connection connection = null;

        try {
            String url = "jdbc:mysql://localhost:3306/clientdatadb";
            String user = "root";
            String password = "manGla1232";
            connection = DriverManager.getConnection(url, user, password);
            System.out.println("Database connection established successfully!");
        } catch (SQLException e) {
            System.err.println("Failed to establish database connection:");
            e.printStackTrace();
            return;

        }


        // cluster 1
        for (int i = 1; i <= 4; i++) {
            int port = 8000 + i; // Ports 8000 to 8004
            Server server = ServerBuilder.forPort(port)
                    .addService(new ServerImpl(("S"+i),1,connection, i))
                    .build()
                    .start();
            System.out.println("Server" + (i + 1) + " started on port " + port);
            servers.add(server);
        }

        // cluster 2
        for (int i = 5; i <= 8; i++) {
            int port = 8000 + i; // Ports 8000 to 8004
            Server server = ServerBuilder.forPort(port)
                    .addService(new ServerImpl(("S"+i),2,connection, i))
                    .build()
                    .start();
            System.out.println("Server" + (i + 1) + " started on port " + port);
            servers.add(server);
        }
//
        // cluster 3
        for (int i = 9; i <= 12; i++) {
            int port = 8000 + i; // Ports 8000 to 8004
            Server server = ServerBuilder.forPort(port)
                    .addService(new ServerImpl(("S"+i),3,connection, i))
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