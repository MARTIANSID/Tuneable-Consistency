package org.example.Utility;
import org.ds.paxos.ClientMessage;
import java.util.List;

public class ProcessResult {
    public final List<ClientMessage> messages;
    public final double tokensUsed;
    public final double profit;

    public int transactionsUpgraded;

    public ProcessResult(List<ClientMessage> messages, double tokensUsed, double profit, int transactionsUpgraded) {
        this.messages = messages;
        this.tokensUsed = tokensUsed;
        this.profit = profit;
        this.transactionsUpgraded = transactionsUpgraded;
    }
}