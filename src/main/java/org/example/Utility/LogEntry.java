package org.example.Utility;

import org.ds.paxos.Transaction;

public class LogEntry {
    public int index;
    public int term;
    public Transaction t;

    public LogEntry(int index, int term, Transaction t) {
        this.index = index;
        this.term = term;
        this.t = t;
    }
    @Override
    public String toString() {
        return "The Transaction is: " + t;
    }
}
