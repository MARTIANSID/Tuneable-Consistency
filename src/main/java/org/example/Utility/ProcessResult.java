package org.example.Utility;
import org.example.raft.ClientMessage;
import java.util.ArrayList;
import java.util.List;

public class ProcessResult {
    public final List<ClientMessage> messages;
    public final double tokensUsed;
    public final double profit;

    public int transactionsUpgraded;

    public double totalTokensUsed;

    // Transactions that were deferred (not executed this round)
    public final List<TransactionOption> deferredTransactions;

    public ProcessResult(List<ClientMessage> messages, double tokensUsed, double profit, int transactionsUpgraded, double totalTokensUsed) {
        this.messages = messages;
        this.tokensUsed = tokensUsed;
        this.profit = profit;
        this.transactionsUpgraded = transactionsUpgraded;
        this.totalTokensUsed = totalTokensUsed;
        this.deferredTransactions = new ArrayList<>();
    }

    public ProcessResult(List<ClientMessage> messages, double tokensUsed, double profit, int transactionsUpgraded, List<TransactionOption> deferredTransactions, double totalTokensUsed) {
        this.messages = messages;
        this.tokensUsed = tokensUsed;
        this.profit = profit;
        this.transactionsUpgraded = transactionsUpgraded;
        this.deferredTransactions = deferredTransactions;
        this.totalTokensUsed = totalTokensUsed;
    }
}