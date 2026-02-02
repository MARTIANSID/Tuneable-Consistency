package org.example.Server;

import org.ds.paxos.ClientMessage;
import org.example.Utility.ServerStatus;
import org.example.Utility.TransactionOption;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Helper class to inject transactions into servers for testing and debugging
 */
public class TransactionInjector {
    private List<ServerImpl> servers;

    public TransactionInjector(List<ServerImpl> serversList) {
        this.servers = serversList;
    }

    /**
     * Get a server by its ID
     */
    public ServerImpl getServerById(int serverId) {
        if (servers == null || serverId < 0 || serverId >= servers.size()) {
            System.err.println("❌ Server " + serverId + " not found");
            return null;
        }
        return servers.get(serverId);
    }

    /**
     * Get the current leader server
     */
    public ServerImpl getLeader() {
        if (servers == null) {
            return null;
        }
        for (ServerImpl server : servers) {
            if (server.status == ServerStatus.ServerCurrentStatus.LEADER) {
                return server;
            }
        }
        return null;
    }

    /**
     * Inject multiple transactions into a specific server
     */
    public void injectIntoServer(int serverId, List<ClientMessage> transactions) {
        ServerImpl server = getServerById(serverId);
        if (server == null) return;

        List<TransactionOption> transactionOptions = transactions.stream()
                .map(TransactionOption::fromClientMessage)
                .collect(Collectors.toList());

        server.batchLock.lock();
        try {
            server.batchOfTransactions.addAll(transactionOptions);
        } finally {
            server.batchLock.unlock();
        }
    }

    /**
     * Inject multiple transactions into the leader
     */
    public void injectIntoLeader(List<ClientMessage> transactions) {
        ServerImpl leader = getLeader();
        if (leader == null) return;

        List<TransactionOption> transactionOptions = transactions.stream()
                .map(TransactionOption::fromClientMessage)
                .collect(Collectors.toList());

        leader.batchLock.lock();
        try {
            leader.batchOfTransactions.addAll(transactionOptions);
        } finally {
            leader.batchLock.unlock();
        }
    }

    /**
     * Inject a single transaction into a specific server
     */
    public void injectSingleIntoServer(int serverId, ClientMessage transaction) {
        ServerImpl server = getServerById(serverId);
        if (server == null) return;

        server.batchLock.lock();
        try {
            server.batchOfTransactions.add(TransactionOption.fromClientMessage(transaction));
            System.out.println("✓ Injected 1 transaction into Server " + serverId);
        } finally {
            server.batchLock.unlock();
        }
    }

    /**
     * Inject a single transaction into the leader
     */
    public void injectSingleIntoLeader(ClientMessage transaction) {
        ServerImpl leader = getLeader();
        if (leader == null) return;

        leader.batchLock.lock();
        try {
            leader.batchOfTransactions.add(TransactionOption.fromClientMessage(transaction));
            System.out.println("✓ Injected 1 transaction into Leader (Server " + leader.serverId + ")");
        } finally {
            leader.batchLock.unlock();
        }
    }

    /**
     * Get the batch queue size of a specific server
     */
    public int getBatchQueueSize(int serverId) {
        ServerImpl server = getServerById(serverId);
        if (server == null) return -1;
        return server.batchOfTransactions.size();
    }

    /**
     * Get the batch queue size of the leader
     */
    public int getLeaderBatchQueueSize() {
        ServerImpl leader = getLeader();
        if (leader == null) return -1;
        return leader.batchOfTransactions.size();
    }

    /**
     * Print status of all servers
     */
    public void printAllServersStatus() {
        if (servers == null || servers.isEmpty()) {
            System.out.println("❌ Servers not initialized");
            return;
        }
        System.out.println("\n========== Server Status ==========");
        for (ServerImpl server : servers) {
            System.out.printf("Server %d: Status=%s | Term=%d | Log Size=%d | Batch Queue=%d%n",
                    server.serverId,
                    server.status,
                    server.currentTerm.get(),
                    server.log.size(),
                    server.batchOfTransactions.size());
        }
        System.out.println("===================================\n");
    }

    /**
     * Wait for a leader to be elected (with timeout)
     */
    public ServerImpl waitForLeader(long timeoutMs) {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            ServerImpl leader = getLeader();
            if (leader != null) {
                return leader;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        System.err.println("❌ Timeout waiting for leader");
        return null;
    }

    /**
     * Clear the batch queue of a specific server
     */
    public void clearBatchQueue(int serverId) {
        ServerImpl server = getServerById(serverId);
        if (server == null) return;

        server.batchLock.lock();
        try {
            int size = server.batchOfTransactions.size();
            server.batchOfTransactions.clear();
            System.out.println("✓ Cleared " + size + " transactions from Server " + serverId + " batch queue");
        } finally {
            server.batchLock.unlock();
        }
    }

    /**
     * Get all servers
     */
    public List<ServerImpl> getAllServers() {
        return servers;
    }
}
