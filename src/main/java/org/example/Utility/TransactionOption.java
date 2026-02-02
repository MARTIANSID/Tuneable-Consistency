package org.example.Utility;

import org.ds.paxos.ClientMessage;
import org.ds.paxos.Transaction;

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

    public TransactionOption(ClientMessage clientMessage, int minRequiredConsistency, double baseProfit, double extraMajorityProfit, double extraIntermediateProfit, String clientHost, int clientPort) {
        this.minRequiredConsistency = minRequiredConsistency;
        this.baseProfit = baseProfit;
        this.extraMajorityProfit = extraMajorityProfit;
        this.clientMessage = clientMessage;
        this.extraIntermediateProfit = extraIntermediateProfit;
        this.clientHost = clientHost;
        this.clientPort = clientPort;
        this.retryCount = 0;
    }

    /**
     * Create a TransactionOption from a ClientMessage (for adding to queue)
     */
    public static TransactionOption fromClientMessage(ClientMessage cm) {
        Transaction t = cm.getT();
        TransactionOption txOption = new TransactionOption(
            cm,
            t.getMinRequiredConsistency(),
            t.getBaseProfit(),
            t.getExtraProfitMajority(),
            t.getExtraIntermediateProfit(),
            cm.getCallbackHost(),
            cm.getCallbackPort()
        );
        txOption.id = t.getId();  // Set the transaction ID
        return txOption;
    }

    public static List<TransactionOption> convertToTransactionOption(List<ClientMessage> clientMessageList) {

        List<TransactionOption> transactionOptionList = new ArrayList<>();

        for(ClientMessage cm : clientMessageList) {
            Transaction t = cm.getT();
            transactionOptionList.add(new TransactionOption(cm,t.getMinRequiredConsistency(), t.getBaseProfit(), t.getExtraProfitMajority(), t.getExtraIntermediateProfit(),cm.getCallbackHost(), cm.getCallbackPort() ));
        }
        return transactionOptionList;
    }
}


