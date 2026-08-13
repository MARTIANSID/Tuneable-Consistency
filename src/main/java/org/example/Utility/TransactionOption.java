package org.example.Utility;

import org.example.raft.ClientMessage;
import org.example.raft.ReadConcern;
import org.example.raft.ReadLevel;
import org.example.raft.Transaction;

import java.util.ArrayList;
import java.util.List;

public class TransactionOption {

    public ClientMessage clientMessage;

    public int minRequiredConsistency;
    public double baseProfit;

    public double extraMajorityProfit;

    public double extraIntermediateProfit;

    public String clientHost;
    public int clientPort;

    public String id;

    // Track how many times this transaction has been deferred
    public int retryCount = 0;

    public int applicationId;

    public boolean isReadOnly;

    // Read transaction fields
    public ReadConcern readConcern;
    public ReadLevel readLevel;
    public String accNameToRead;

    public TransactionOption(ClientMessage clientMessage, int minRequiredConsistency, double baseProfit, double extraMajorityProfit, double extraIntermediateProfit, String clientHost, int clientPort, int applicationId, boolean isReadOnly) {
        this.minRequiredConsistency = minRequiredConsistency;
        this.baseProfit = baseProfit;
        this.extraMajorityProfit = extraMajorityProfit;
        this.clientMessage = clientMessage;
        this.extraIntermediateProfit = extraIntermediateProfit;
        this.clientHost = clientHost;
        this.clientPort = clientPort;
        this.retryCount = 0;
        this.applicationId = applicationId;
        this.isReadOnly = isReadOnly;
    }
    
    /**
     * Create a TransactionOption from a ClientMessage (for adding to queue)
     */
    public static TransactionOption fromClientMessage(ClientMessage cm) {
        Transaction t = cm.getT();
        t = t.toBuilder().setTransactionArrivalTimeOnLeader(System.currentTimeMillis()).build();
        cm = cm.toBuilder().setT(t).build();
        // t = t.newBuilder().setTransactionArrivalTimeOnLeader(System.currentTimeMillis()).build();
        // cm = cm.newBuilder().setT(t).build();

        TransactionOption txOption = new TransactionOption(
            cm,
            t.getMinRequiredConsistency(),
            t.getBaseProfit(),
            t.getExtraProfitMajority(),
            t.getExtraIntermediateProfit(),
            cm.getCallbackHost(),
            cm.getCallbackPort(),
            t.getApplicationId(),
            t.getIsReadOnly()
        );
        txOption.id = t.getId();  // Set the transaction ID
        // Populate read transaction fields
        txOption.readConcern = t.getReadConcern();
        txOption.readLevel = t.getReadLevel();
        txOption.accNameToRead = t.getAccNameToRead();
        return txOption;
    }
}


