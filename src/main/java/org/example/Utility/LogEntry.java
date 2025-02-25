package org.example.Utility;

import org.ds.paxos.Transaction;

public class LogEntry {
    public int index;
    public int term;
    public Transaction t;
    public int writeConcern = 2;

    public LogEntry(int index, int term, Transaction t, int writeConcern) {
        this.index = index;
        this.term = term;
        this.t = t;
        this.writeConcern = writeConcern;
    }
    @Override
    public String toString() {
        return "The Transaction is: " + t;
    }
}
