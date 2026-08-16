package org.example.Server;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.example.Utility.ExperimentConfig;

import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * Entry point for a single Raft node running in its own OS process: the Raft
 * service, the client-facing KV service, and the admin service (leader
 * detection, failure injection, teardown) on serverBasePort + serverId + 1.
 * The workload driver (org.example.Client.WorkloadDriver) runs in a separate
 * process and stops this one over Admin.Shutdown; SIGTERM performs the same
 * orderly teardown via a JVM shutdown hook.
 */
public final class ServerNode {

    private ServerNode() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: ServerNode <config.yaml|config.json> <serverId>");
            System.exit(64);
        }
        Path configPath = Path.of(args[0]);
        int serverId = Integer.parseInt(args[1]);

        ExperimentConfig config = ExperimentConfig.load(configPath);
        int numServers = config.cluster.numServers;
        if (serverId < 0 || serverId >= numServers) {
            throw new IllegalArgumentException(
                    "serverId " + serverId + " out of range for a " + numServers + "-node cluster");
        }
        ServerImpl.applyConfig(config);
        KvClientService.applyConfig(config);
        MeasurementPlane.applyConfig(config);
        SlaRegistry.applyConfig(config);

        clearOwnCsvFiles(serverId);

        int port = config.cluster.serverBasePort + serverId + 1;
        ServerImpl serverImpl = new ServerImpl(serverId, numServers);
        MeasurementPlane plane = new MeasurementPlane(serverId, numServers);
        plane.setRoleSupplier(() -> serverImpl.status.name());

        AtomicReference<Server> serverRef = new AtomicReference<>();
        AtomicBoolean stopping = new AtomicBoolean(false);
        // Runs at most once, from the Admin.Shutdown RPC or the SIGTERM hook,
        // whichever comes first.
        Runnable shutdownAction = () -> {
            if (!stopping.compareAndSet(false, true)) {
                return;
            }
            System.out.println("Server" + serverId + " shutting down...");
            Server server = serverRef.get();
            server.shutdown();
            try {
                if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                server.shutdownNow();
            }
            serverImpl.shutdown();
            plane.close();
        };

        Server server = ServerBuilder.forPort(port)
                .addService(serverImpl)
                .addService(new KvClientService(serverImpl, plane))
                .addService(new AdminService(serverImpl, shutdownAction))
                .build()
                .start();
        serverRef.set(server);
        serverImpl.setUpStubs();
        Runtime.getRuntime().addShutdownHook(new Thread(shutdownAction, "server-node-shutdown"));
        System.out.println("Server" + serverId + " started on port " + port
                + " (pid " + ProcessHandle.current().pid() + ")");

        server.awaitTermination();
        System.out.println("Server" + serverId + " stopped.");
    }

    /** Clear this node's result CSVs at startup so every run starts clean. */
    private static void clearOwnCsvFiles(int serverId) {
        for (String filename : new String[] {
                "occupancy_" + serverId + ".csv",
                "histograms_" + serverId + ".csv" }) {
            File file = new File(filename);
            if (file.exists() && !file.delete()) {
                System.out.println("Warning: could not delete " + filename);
            }
        }
    }
}
