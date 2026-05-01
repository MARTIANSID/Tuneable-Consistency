package org.example.Utility;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.ds.rl.AckResponse;
import org.ds.rl.BudgetRequest;
import org.ds.rl.BudgetResponse;
import org.ds.rl.Experience;
import org.ds.rl.HealthRequest;
import org.ds.rl.HealthResponse;
import org.ds.rl.RLBudgetServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

/**
 * gRPC Client to communicate with Python RL Model Server
 * Provides budget predictions and sends training experiences
 */
public class RLModelGrpcClient {
    
    private final ManagedChannel channel;
    private final RLBudgetServiceGrpc.RLBudgetServiceBlockingStub blockingStub;
    private final boolean fallbackEnabled;
    private final double fallbackBudget;
    
    // Track previous state for experience recording
    private double[] previousState = null;
    private double previousBudget = 0;
    private long previousTimestamp = 0;
    
    /**
     * Constructor with default settings
     */
    public RLModelGrpcClient(String host, int port) {
        this(host, port, true, 10000.0);
    }
    
    /**
     * Constructor with custom settings
     * 
     * @param host gRPC server host
     * @param port gRPC server port
     * @param fallbackEnabled if true, return fallback budget on error
     * @param fallbackBudget default budget to return if model call fails
     */
    public RLModelGrpcClient(String host, int port, boolean fallbackEnabled, double fallbackBudget) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        
        this.blockingStub = RLBudgetServiceGrpc.newBlockingStub(channel);
        this.fallbackEnabled = fallbackEnabled;
        this.fallbackBudget = fallbackBudget;
    }
    
    /**
     * Get budget prediction and automatically record experience for training
     * 
     * @param currentLoad number of transactions in current batch
     * @param currentThroughput recent throughput (transactions/sec)
     * @param currentBacklog number of transactions waiting
     * @param writeConcernCosts map of write concern level to cost
     * @param capacity system capacity (e.g., number of tokens)
     * @return predicted budget value
     */
    public double predictBudgetAndRecord(
            int currentLoad,
            double currentThroughput,
            int currentBacklog,
            HashMap<Integer, Double> writeConcernCosts,
            double capacity,
            ProcessResult previousResult  // Result from previous batch
    ) {
        try {
            // Calculate current state features
            double avgWcCost = writeConcernCosts.values().stream()
                    .mapToDouble(Double::doubleValue)
                    .average()
                    .orElse(0.0);
            
            // Replace utilization with incoming load (txns/sec)
            double incomingLoadPerSec = currentLoad;
            
            double[] currentState = new double[] {
                currentLoad,
                currentThroughput,
                currentBacklog,
                avgWcCost,
                incomingLoadPerSec
            };
            
            // If we have previous state, record experience for training
            if (previousState != null && previousResult != null) {
                recordExperience(
                        previousState,
                        currentState,
                        previousBudget,
                        previousResult
                );
            }
            
            // Get prediction for current state
            BudgetRequest request = BudgetRequest.newBuilder()
                    .setCurrentLoad(currentLoad)
                    .setCurrentThroughput(currentThroughput)
                    .setCurrentBacklog(currentBacklog)
                    .setAvgWcCost(avgWcCost)
                    // Send incoming load per second in utilization field for compatibility
                    .setUtilization(incomingLoadPerSec)
                    .build();
            
            BudgetResponse response = blockingStub.predictBudget(request);
            
            if (response.getSuccess()) {
                double predictedBudget = response.getPredictedBudget();
                System.out.println("RL Model predicted budget: " + predictedBudget + 
                        " (confidence: " + String.format("%.2f", response.getConfidence()) + ")");
                
                // Save current state for next experience recording
                previousState = currentState;
                previousBudget = predictedBudget;
                previousTimestamp = System.currentTimeMillis();
                
                return predictedBudget;
            } else {
                System.err.println("RL Model returned error: " + response.getErrorMessage());
                return getFallbackBudget();
            }
            
        } catch (StatusRuntimeException e) {
            System.err.println("gRPC call failed: " + e.getStatus());
            return getFallbackBudget();
        } catch (Exception e) {
            System.err.println("Error calling RL model: " + e.getMessage());
            return getFallbackBudget();
        }
    }
    
    /**
     * Record experience for online training
     */
    private void recordExperience(
            double[] prevState,
            double[] nextState,
            double budgetUsed,
            ProcessResult result
    ) {
        try {
            Experience experience = Experience.newBuilder()
                    // Previous state
                    .setCurrentLoad(prevState[0])
                    .setCurrentThroughput(prevState[1])
                    .setCurrentBacklog(prevState[2])
                    .setAvgWcCost(prevState[3])
                    .setUtilization(prevState[4])
                    // Action
                    .setBudgetUsed(budgetUsed)
                    // Result
                    .setProfit(result.profit)
                    .setTransactionsProcessed(result.messages.size())
                    .setTransactionsUpgraded(result.transactionsUpgraded)
                    // Next state
                    .setNextLoad(nextState[0])
                    .setNextThroughput(nextState[1])
                    .setNextBacklog(nextState[2])
                    .setNextAvgWcCost(nextState[3])
                    .setNextUtilization(nextState[4])
                    // Metadata
                    .setTimestamp(System.currentTimeMillis())
                    .build();
            
            // Send asynchronously (non-blocking)
            new Thread(() -> {
                try {
                    AckResponse ack = blockingStub.recordExperience(experience);
                    if (ack.getSuccess()) {
                        System.out.println("Experience recorded: " + ack.getMessage());
                    }
                } catch (Exception e) {
                    System.err.println("Failed to record experience: " + e.getMessage());
                }
            }).start();
            
        } catch (Exception e) {
            System.err.println("Error creating experience: " + e.getMessage());
        }
    }
    
    /**
     * Check if the model server is healthy
     */
    public boolean isHealthy() {
        try {
            HealthRequest request = HealthRequest.newBuilder().build();
            HealthResponse response = blockingStub.healthCheck(request);
            
            System.out.println("RL Model Status:");
            System.out.println("  Healthy: " + response.getIsHealthy());
            System.out.println("  Model Loaded: " + response.getModelLoaded());
            System.out.println("  Training Samples: " + response.getTrainingSamplesCount());
            System.out.println("  Epsilon: " + String.format("%.4f", response.getEpsilon()));
            System.out.println("  Training Steps: " + response.getTrainingSteps());
            
            return response.getIsHealthy();
        } catch (Exception e) {
            System.err.println("Health check failed: " + e.getMessage());
            return false;
        }
    }
    
    /**
     * Get fallback budget when model is unavailable
     */
    private double getFallbackBudget() {
        if (fallbackEnabled) {
            System.out.println("Using fallback budget: " + fallbackBudget);
            return fallbackBudget;
        } else {
            throw new RuntimeException("RL Model call failed and fallback is disabled");
        }
    }
    
    /**
     * Shutdown the channel
     */
    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
}
