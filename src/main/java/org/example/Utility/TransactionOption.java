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

   public String id;

    public TransactionOption(ClientMessage clientMessage, int minRequiredConsistency, double baseProfit, double extraMajorityProfit) {
        this.minRequiredConsistency = minRequiredConsistency;
        this.baseProfit = baseProfit;
        this.extraMajorityProfit = extraMajorityProfit;
        this.clientMessage = clientMessage;
    }

    public static List<TransactionOption> convertToTransactionOption(List<ClientMessage> clientMessageList) {

        List<TransactionOption> transactionOptionList = new ArrayList<>();

        for(ClientMessage cm : clientMessageList) {
            Transaction t = cm.getT();
            transactionOptionList.add(new TransactionOption(cm,t.getMinRequiredConsistency(), t.getBaseProfit(), t.getExtraProfitMajority()));
        }
        return transactionOptionList;
    }
}


