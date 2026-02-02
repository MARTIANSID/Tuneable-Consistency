package org.example.Utility;
import org.ds.paxos.ClientMessage;
import java.util.ArrayList;
import java.util.List;

public class ProcessResult {
    public final List<ClientMessage> messages;
    public final double tokensUsed;
    public final double profit;

    public int transactionsUpgraded;

    // Transactions that were deferred (not executed this round)
    public final List<TransactionOption> deferredTransactions;

    public ProcessResult(List<ClientMessage> messages, double tokensUsed, double profit, int transactionsUpgraded) {
        this.messages = messages;
        this.tokensUsed = tokensUsed;
        this.profit = profit;
        this.transactionsUpgraded = transactionsUpgraded;
        this.deferredTransactions = new ArrayList<>();
    }

    public ProcessResult(List<ClientMessage> messages, double tokensUsed, double profit, int transactionsUpgraded, List<TransactionOption> deferredTransactions) {
        this.messages = messages;
        this.tokensUsed = tokensUsed;
        this.profit = profit;
        this.transactionsUpgraded = transactionsUpgraded;
        this.deferredTransactions = deferredTransactions;
    }
}